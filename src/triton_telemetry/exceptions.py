"""Excepciones semánticas de dominio del Proyecto Tritón.

Responsable: Gabriel — Ingeniero de Robustez de Entradas y Excepciones.

Contribución de Gabriel:
    Este módulo centraliza la jerarquía de errores de dominio y la traducción
    de fallos técnicos a excepciones semánticas reutilizables por la capa de
    orquestación. La intención es que la lógica de negocio sepa distinguir entre
    timeouts de proveedor, payload corrupto y pérdidas de peering sin depender
    de detalles genuinos de la infraestructura de red.

Este módulo define el mapeo semántico de errores del sistema de telemetría.
Su objetivo es traducir fallos técnicos de bajo nivel (excepciones nativas de
``httpx``, del subsistema DNS o del deserializador JSON) a un vocabulario de
dominio que la capa de orquestación pueda capturar de forma quirúrgica con
``except*``.

Decisión de diseño crítica
--------------------------
Toda la jerarquía cuelga de :class:`Exception` y **nunca** de
:class:`BaseException`. Heredar de ``BaseException`` provocaría que un bloque
``except TritonError`` secuestrara señales vitales del sistema operativo como
:class:`KeyboardInterrupt` (``Ctrl+C``) o :class:`SystemExit`, dejando al
operador sin capacidad de abortar un proceso desatendido colgado.
"""

from __future__ import annotations

__author__ = "Gabriel"
__responsable__ = "Gabriel - Ingeniero de Robustez de Entradas y Excepciones"

__all__ = [
    "TritonError",
    "ProviderTimeoutError",
    "CorruptedPayloadError",
    "NetworkPeeringError",
]


class TritonError(Exception):
    """Excepción raíz de todos los fallos de dominio del Proyecto Tritón.

    Hereda de ``Exception`` de forma deliberada para no interceptar señales de
    control del sistema operativo.

    Args:
        mensaje: Descripción legible del fallo de dominio.
        proveedor: Nube afectada (``AWS``, ``Azure`` o ``GCP``). Opcional.
        endpoint: URL real que se estaba interrogando cuando ocurrió el fallo.

    Attributes:
        mensaje: Texto base del incidente, sin decoración.
        proveedor: Identificador del proveedor cloud afectado.
        endpoint: Endpoint HTTP involucrado en el incidente.
    """

    def __init__(
        self,
        mensaje: str,
        *,
        proveedor: str | None = None,
        endpoint: str | None = None,
    ) -> None:
        super().__init__(mensaje)
        self.mensaje = mensaje
        self.proveedor = proveedor
        self.endpoint = endpoint

    def __str__(self) -> str:
        """Antepone el proveedor afectado para facilitar la lectura en consola."""
        if self.proveedor:
            return f"[{self.proveedor}] {self.mensaje}"
        return self.mensaje


class ProviderTimeoutError(TritonError):
    """El nodo de telemetría no respondió dentro de la ventana de espera.

    Se emite cuando ``httpx`` levanta cualquier variante de
    ``httpx.TimeoutException`` (conexión, lectura, escritura o pool).

    Args:
        mensaje: Descripción del incidente.
        proveedor: Nube afectada.
        endpoint: URL interrogada.
        segundos_limite: Ventana de espera configurada que fue superada.
    """

    def __init__(
        self,
        mensaje: str,
        *,
        proveedor: str | None = None,
        endpoint: str | None = None,
        segundos_limite: float | None = None,
    ) -> None:
        super().__init__(mensaje, proveedor=proveedor, endpoint=endpoint)
        self.segundos_limite = segundos_limite


class CorruptedPayloadError(TritonError):
    """La respuesta llegó, pero su contenido o su estatus HTTP es inutilizable.

    Cubre dos situaciones distintas del escenario de producción:

    1. El servidor devolvió un código de estado no controlado (``4xx`` / ``5xx``)
       detectado mediante ``response.raise_for_status()``.
    2. El cuerpo de la respuesta no es deserializable como JSON (por ejemplo,
       una tormenta de radiación que hace responder al nodo con XML).

    Args:
        mensaje: Descripción del incidente.
        proveedor: Nube afectada.
        endpoint: URL interrogada.
        codigo_estado: Código HTTP recibido, si lo hubo.
        tipo_contenido: Cabecera ``Content-Type`` observada, si la hubo.
    """

    def __init__(
        self,
        mensaje: str,
        *,
        proveedor: str | None = None,
        endpoint: str | None = None,
        codigo_estado: int | None = None,
        tipo_contenido: str | None = None,
    ) -> None:
        super().__init__(mensaje, proveedor=proveedor, endpoint=endpoint)
        self.codigo_estado = codigo_estado
        self.tipo_contenido = tipo_contenido


class NetworkPeeringError(TritonError):
    """Fallo catastrófico de conectividad: pérdida de peering o de resolución DNS.

    Se emite cuando el host no resuelve, cuando el transporte se cae antes de
    completar el intercambio o cuando directamente no hay salida a internet.

    Args:
        mensaje: Descripción del incidente.
        proveedor: Nube afectada.
        endpoint: URL interrogada.
        host: Nombre de host que no pudo resolverse o alcanzarse.
    """

    def __init__(
        self,
        mensaje: str,
        *,
        proveedor: str | None = None,
        endpoint: str | None = None,
        host: str | None = None,
    ) -> None:
        super().__init__(mensaje, proveedor=proveedor, endpoint=endpoint)
        self.host = host
