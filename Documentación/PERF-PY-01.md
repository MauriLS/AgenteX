# PERF-PY-01 — Socket exhaustion por `httpx.AsyncClient` no reutilizado

| Campo | Valor |
|---|---|
| **ID** | PERF-PY-01 |
| **Componente** | `motor-ia-agenteX` (microservicio Python/FastAPI) |
| **Severidad** | Alta — el endpoint queda inutilizable bajo concurrencia mínima |
| **Tipo** | Deuda técnica de performance |
| **Estado** | 🔴 Pendiente — no implementado antes de la entrega del EA2 |
| **Detectado en** | Carga k6 (`tests/load/motor-ia.k6.js`), corrida real del 2026-06-20 |

---

## 1. Descripción del problema

`POST /api/ia/process` es el único endpoint del microservicio. Bajo carga concurrente, su latencia se degrada de forma catastrófica (de milisegundos a decenas de segundos) y empieza a fallar con errores de conexión, pese a que tanto DeepSeek como el ERP estaban representados por *stubs* locales triviales (sin lógica de negocio, sin red externa real). El endpoint de chat queda, en la práctica, inutilizable apenas se supera una concurrencia mínima (10 VUs).

## 2. Evidencia (métricas k6 reales)

Corrida real contra el microservicio levantado localmente (`uvicorn main:app`), con `tests/load/stub_server.py` sirviendo de stub de DeepSeek + ERP. Diseño: 4 escenarios secuenciales (10 VUs / 50 VUs / 100 VUs constantes de 30s, luego rampa de estrés 0→200 VUs con auto-abort si `http_req_failed` > 5%).

```
THRESHOLDS
  http_req_duration   ✗ p(95)<300ms  -> p(95)=57.75s
  http_req_failed     ✗ rate<0.05    -> rate=14.94%
  http_req_failed{scenario:escenario_estres} ✗ -> rate=100.00%

checks_succeeded: 100% (148/148 — todo lo que completó fue correcto)
iterations: 74 completas, 284 interrumpidas por el abort del escenario de estrés (~161/200 VUs)
```

Ya en el escenario de **10 VUs** (el primero y más liviano de los cuatro) el p95 alcanzó ~57s, muy por encima del umbral objetivo de 300ms — el síntoma aparece con la concurrencia más baja probada, no solo en el escenario de estrés.

Durante la corrida, los logs de `uvicorn main:app` mostraron una tormenta de `ConnectionResetError` repetidos en `_ProactorBasePipeTransport._call_connection_lost` (asyncio, Windows) — consistente con apertura/cierre masivo de sockets efímeros, no con lentitud de cómputo.

## 3. Causa raíz

Tanto [`main.py`](../main.py) (llamada a DeepSeek) como [`tools.py`](../tools.py) (llamada al ERP) abren un **`httpx.AsyncClient()` nuevo en cada llamada saliente**, dentro del loop de orquestación, en vez de reutilizar un cliente compartido con pool de conexiones:

- `main.py:95` — dentro del `while iteration < MAX_ITERATIONS:`, cada iteración hacia DeepSeek crea su propio cliente:
  ```python
  async with httpx.AsyncClient(timeout=45.0) as client:
      response = await client.post(DEEPSEEK_API_URL, headers=headers, json=payload)
  ```
- `tools.py:111` — cada invocación de `consultar_inventario_erp` (una por cada vez que el modelo decide usar la tool) crea otro cliente independiente:
  ```python
  async with httpx.AsyncClient(timeout=30.0) as client:
      response = await client.get(url)
  ```

Cada `httpx.AsyncClient()` administra su propio pool de conexiones TCP. Al instanciarlo por request (y por iteración del loop de tools) en vez de a nivel de aplicación, cada llamada saliente abre y cierra su propio socket en vez de reutilizar conexiones ya establecidas. Bajo concurrencia, esto agota los sockets efímeros disponibles (*socket exhaustion*) — ineficiente en cualquier sistema operativo, y en Windows además dispara el bug conocido de `asyncio` con el *Proactor event loop* ante alta tasa de conexiones de vida corta (los `ConnectionResetError` observados en los logs).

## 4. Solución propuesta: cliente singleton vía `lifespan` de FastAPI

Reemplazar la creación de `AsyncClient` por-llamada con un único cliente compartido, creado una vez al arrancar la app y reutilizado por todas las requests, con un pool de conexiones configurado explícitamente.

### `main.py`

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    app.state.http_client = httpx.AsyncClient(timeout=45.0, limits=limits)
    yield
    await app.state.http_client.aclose()

app = FastAPI(title="Agente X - Motor IA (Stateless Worker)", lifespan=lifespan)
```

Dentro de `process_chat`, reemplazar el `async with httpx.AsyncClient(...) as client:` por el cliente del estado de la app:

```python
client = req.app.state.http_client if False else None  # ver nota
```

> Nota: como `process_chat` no recibe `Request`/`app` directamente hoy, la forma más simple es agregar `request: Request` como parámetro del endpoint y usar `request.app.state.http_client`, o exponer el cliente como variable de módulo asignada en `lifespan`. Ejemplo con `Request`:

```python
from fastapi import Request

@app.post("/api/ia/process")
async def process_chat(
    req: ChatRequest,
    request: Request,
    x_internal_secret: Optional[str] = Header(None, alias="X-Internal-Secret"),
):
    client = request.app.state.http_client
    ...
    response = await client.post(DEEPSEEK_API_URL, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()
```

### `tools.py`

`consultar_inventario_erp` debe recibir el cliente compartido en vez de crear el suyo:

```python
async def consultar_inventario_erp(
    client:            httpx.AsyncClient,
    tipo_filtro:       str,
    valor_busqueda:    str,
    erp_url:           str,
    erp_mapping:       dict = None,
    categoria_refinada: str = None,
) -> str:
    ...
    response = await client.get(url)
    ...
```

Y en `main.py`, pasar el mismo `client` al invocarla:

```python
tool_result = await consultar_inventario_erp(
    client=client,
    tipo_filtro=arguments.get("tipo_filtro"),
    valor_busqueda=arguments.get("valor_busqueda"),
    erp_url=req.erp_url,
    erp_mapping=req.erp_mapping,
    categoria_refinada=arguments.get("categoria_refinada"),
)
```

Con esto, una sola conexión TCP (o un pool pequeño con keep-alive) se reutiliza para todas las llamadas a DeepSeek y al ERP durante la vida del proceso, sin abrir/cerrar sockets en cada request.

### Impacto en los tests existentes

Los tests actuales (`conftest.py`, `respx.mock`) interceptan a nivel de transporte HTTP, no el constructor de `AsyncClient`, por lo que deberían seguir funcionando sin cambios mayores. Sí habrá que ajustar las fixtures que construyen la app de test para que el `lifespan` se ejecute (httpx `ASGITransport` lo respeta si se usa `httpx.AsyncClient(app=app)` con el lifecycle activado, o invocando el lifespan manualmente en el fixture).

## 5. Impacto estimado del fix

- Elimina la apertura/cierre de sockets por request y por iteración del loop de tools — la causa directa del agotamiento de sockets observado.
- Debería traer el p95 de vuelta a un orden de magnitud consistente con la latencia real del stub/DeepSeek (decenas/cientos de ms, no decenas de segundos).
- Debería evitar el aborto del escenario de estrés (200 VUs) y bajar la tasa de error (`http_req_failed`) muy por debajo del 14.94% medido.
- No cambia ninguna lógica de negocio — es un cambio de gestión de recursos, no de comportamiento del endpoint.
- No se puede confirmar el número exacto sin volver a correr `tests/load/motor-ia.k6.js` tras el fix.

## 6. Estado actual

**Pendiente.** El alcance de la sesión de testing que detectó este hallazgo fue construir y documentar la suite de pruebas, no refactorizar `main.py`/`tools.py`. Queda registrado como mejora concreta para la próxima iteración. Acción de cierre: implementar el patrón `lifespan` descrito arriba y volver a correr `tests/load/motor-ia.k6.js` para confirmar que los thresholds de p95 y `http_req_failed` pasan.
