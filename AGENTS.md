# streambus

Librería Python para consumir y publicar eventos sobre Redis Streams. Agnóstica de framework.

<!-- rule:framework-agnostic -->
## Sin dependencias de framework

streambus no importa Django, FastAPI, Celery ni ningún otro framework. Solo depende de `redis-py`.

El caller es responsable de resolver los valores de configuración (variables de entorno, Django settings, archivos de config) y pasarlos como tipos primitivos a los objetos de streambus. La librería no sabe de dónde vienen.

Nunca añadir imports de frameworks externos al paquete `streambus/`.
<!-- /rule:framework-agnostic -->

<!-- rule:event-contract -->
## Contrato de eventos

Todo evento debe extender `StreamBusEvent`. El publisher rechaza cualquier objeto que no sea una instancia de `StreamBusEvent`. El listener valida en construcción que `event_class` sea una subclase.

```python
from dataclasses import dataclass
from streambus import StreamBusEvent

@dataclass(kw_only=True)
class ClientEvent(StreamBusEvent):
    client_id: str
    name: str
```

`StreamBusEvent` define dos campos base:
- `event_type: str` — obligatorio, identifica el tipo de evento.
- `occurred_at: str` — timestamp ISO 8601, se genera automáticamente si no se pasa.

Cualquier campo adicional lo define la subclase. Los campos del stream que no estén declarados en la subclase se descartan en `from_dict`.
<!-- /rule:event-contract -->

<!-- rule:no-field-defaults -->
## Sin valores por defecto en campos de dominio

Los campos de las subclases de `StreamBusEvent` no llevan valor por defecto. El caller debe pasarlos explícitamente. Esto fuerza que los eventos sean siempre completos y evita estados ambiguos.

Correcto:
```python
@dataclass(kw_only=True)
class ClientEvent(StreamBusEvent):
    client_id: str
    name: str
    is_active: str
```

Incorrecto:
```python
@dataclass(kw_only=True)
class ClientEvent(StreamBusEvent):
    client_id: str = ""   # ← no
    name: str = ""        # ← no
```

La excepción son los campos de configuración interna de la librería: `occurred_at` en `StreamBusEvent` y los timeouts de socket en `RedisConfig`. Estos tienen defaults porque son detalles de infraestructura, no datos de dominio.
<!-- /rule:no-field-defaults -->

<!-- rule:kw-only -->
## kw_only=True obligatorio en todas las clases de evento

`StreamBusEvent` usa `@dataclass(kw_only=True)`. Todas las subclases deben declararse también con `@dataclass(kw_only=True)`.

Esto es necesario porque `StreamBusEvent` tiene `occurred_at` con default. Sin `kw_only=True`, Python rechaza subclases que añadan campos requeridos después de un campo con default.

```python
@dataclass(kw_only=True)          # obligatorio
class ClientEvent(StreamBusEvent):
    client_id: str
    name: str
```
<!-- /rule:kw-only -->

<!-- rule:config-naming -->
## Nombres de campos en objetos de configuración

Los campos dentro de un objeto de configuración tipado no repiten el contexto que ya da el nombre de la clase.

Correcto: `RedisConfig.url` — el tipo ya establece que es Redis.

Incorrecto: `RedisConfig.redis_url` — repite "redis" innecesariamente.

Aplicar esta regla a cualquier objeto de configuración nuevo que se añada a la librería.
<!-- /rule:config-naming -->

<!-- rule:local-event-class -->
## Cada consumer define su propia clase de evento

Los consumers no comparten la definición de `ClientEvent` con el publisher ni entre sí. Cada uno declara únicamente los campos que necesita. Los campos del stream que no aparezcan en la clase local se descartan automáticamente en `from_dict`.

Esto mantiene el acoplamiento al mínimo: un consumer de studio no necesita conocer que curly publica `slug` si no lo usa.

```python
# studio solo necesita estos tres
@dataclass(kw_only=True)
class ClientEvent(StreamBusEvent):
    client_id: str
    name: str
    is_active: str
```
<!-- /rule:local-event-class -->

<!-- rule:exceptions -->
## Jerarquía de excepciones

Usar siempre las excepciones del módulo `streambus.exceptions`:

- `StreambusError` — base de todas las excepciones de la librería.
- `ConfigurationError` — parámetros inválidos o ausentes en construcción de `EventListener` o `EventPublisher`.
- `EventValidationError` — evento inválido en `publish()` (no es `StreamBusEvent`, `event_type` vacío).

No lanzar `ValueError` ni `TypeError` directamente desde código de la librería. Si se añade nueva lógica de validación, subclasificar desde `StreambusError`.
<!-- /rule:exceptions -->
