"""Paquete ``triton_telemetry``: frontera pública del sistema de telemetría.

Este módulo actúa como **frontera de imports** del paquete. Encapsula la
organización interna de los submódulos y declara de forma explícita, mediante
``__all__``, qué objetos se exponen cuando un desarrollador realiza una
importación masiva (``from triton_telemetry import *``).

Todo lo que no figure en ``__all__`` se considera detalle de implementación
privado y puede cambiar sin aviso entre versiones.

Ejemplo de uso::

    from triton_telemetry import escanear_proveedores, ProviderTimeoutError
"""

from __future__ import annotations

from .core import (
    ENDPOINTS_CAOS,
    ENDPOINTS_NOMINALES,
    PROVEEDORES_SOPORTADOS,
    consultar_proveedor,
    escanear_proveedores,
    resolver_endpoint,
)
from .exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,
)
from .logging_engine import (
    ARCHIVO_LOG_POR_DEFECTO,
    AsyncJSONFormatter,
    ColaTelemetria,
    construir_esquema_logging,
    detener_pipeline,
    iniciar_pipeline,
    serializar_excepcion,
)
from .sanitizer import validar_identificador_cluster, validar_timeout

__version__ = "1.0.0"

__all__ = [
    # Excepciones semánticas de dominio
    "TritonError",
    "ProviderTimeoutError",
    "CorruptedPayloadError",
    "NetworkPeeringError",
    # Sanitización de la frontera CLI
    "validar_timeout",
    "validar_identificador_cluster",
    # Motor de observabilidad
    "AsyncJSONFormatter",
    "ColaTelemetria",
    "construir_esquema_logging",
    "iniciar_pipeline",
    "detener_pipeline",
    "serializar_excepcion",
    "ARCHIVO_LOG_POR_DEFECTO",
    # Núcleo asíncrono
    "escanear_proveedores",
    "consultar_proveedor",
    "resolver_endpoint",
    "PROVEEDORES_SOPORTADOS",
    "ENDPOINTS_NOMINALES",
    "ENDPOINTS_CAOS",
    # Metadatos
    "__version__",
]
