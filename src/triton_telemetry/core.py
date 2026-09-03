"""Lógica concurrente asíncrona de red del Proyecto Tritón.

Responsable: **Integrante 2 - Ingeniero de Concurrencia y Telemetría Asíncrona**.

Este módulo interroga en paralelo los nodos de telemetría de AWS, Azure y GCP
mediante peticiones HTTP **reales** contra servicios públicos de internet. No
hay simulaciones sintéticas locales: el sistema mide latencias reales, sufre
timeouts reales y recibe códigos de estado reales.

La orquestación se apoya en :class:`asyncio.TaskGroup`, que garantiza que
ninguna corrutina quede huérfana y que **todos** los fallos concurrentes se
propaguen agrupados en un único :class:`ExceptionGroup` nativo, en lugar de
perderse todos menos el primero.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

import httpx

from .exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,
)

__all__ = [
    "PROVEEDORES_SOPORTADOS",
    "ENDPOINTS_NOMINALES",
    "ENDPOINTS_CAOS",
    "resolver_endpoint",
    "consultar_proveedor",
    "escanear_proveedores",
]

#: Proveedores cloud que el monitor sabe interrogar.
PROVEEDORES_SOPORTADOS: tuple[str, ...] = ("AWS", "Azure", "GCP")

#: Endpoints de operación nominal. Se modela el estado operativo de cada nodo
#: consultando un recurso distinto de la API pública JSONPlaceholder.
ENDPOINTS_NOMINALES: dict[str, str] = {
    "AWS": "https://jsonplaceholder.typicode.com/posts/1",
    "Azure": "https://jsonplaceholder.typicode.com/posts/2",
    "GCP": "https://jsonplaceholder.typicode.com/posts/3",
}

#: Endpoints de inyección de caos (bandera ``--chaos``). Reproducen en caliente
#: los tres modos de colapso descritos en el escenario de producción:
#:
#: * ``AWS``   -> retardo controlado de 3 s que dispara un timeout real.
#: * ``Azure`` -> código de estado 504 Gateway Timeout inyectado por el servidor.
#: * ``GCP``   -> payload corrupto: el nodo responde XML en lugar de JSON.
ENDPOINTS_CAOS: dict[str, str] = {
    "AWS": "https://httpbin.org/delay/3",
    "Azure": "https://httpbin.org/status/504",
    "GCP": "https://httpbin.org/xml",
}

#: Endpoint alternativo de estatus inválido, usado por la suite de caos.
ENDPOINT_ESTATUS_INVALIDO = "https://httpbin.org/status/422"

#: Prefijo de variable de entorno que permite sobreescribir el endpoint de un
#: proveedor concreto. Lo utiliza la suite de simulación de caos del
#: Integrante 6 para apuntar a hosts inexistentes y forzar fallos de peering.
#: Ejemplo: ``TRITON_ENDPOINT_AWS="https://host-inexistente.invalid/status"``.
PREFIJO_ENDPOINT_ENV = "TRITON_ENDPOINT_"


def resolver_endpoint(proveedor: str, *, modo_caos: bool = False) -> str:
    """Determina qué URL real debe interrogarse para un proveedor dado.

    El orden de precedencia es: variable de entorno de override, tabla de caos
    (si la bandera está activa), y finalmente la tabla nominal.
    La variable de entorno tiene máxima prioridad y permite inyectar
    comportamientos externos sin modificar el código.

    Args:
        proveedor: Identificador del proveedor (``AWS``, ``Azure`` o ``GCP``).
        modo_caos: Si es ``True``, usa la tabla de endpoints de caos.

    Returns:
        URL absoluta a consultar.

    Raises:
        KeyError: Si el proveedor no está soportado.
    """
    override = os.environ.get(f"{PREFIJO_ENDPOINT_ENV}{proveedor.upper()}")
    if override:
        return override

    tabla = ENDPOINTS_CAOS if modo_caos else ENDPOINTS_NOMINALES
    return tabla[proveedor]


def _traducir_error_httpx(
    error: Exception,
    proveedor: str,
    url: str,
    timeout: float,
    respuesta: httpx.Response | None = None,
) -> TritonError:
    """Convierte una excepción nativa de httpx en una excepción de dominio
    enriquecida con notas forenses.

    Args:
        error: Excepción lanzada por el cliente HTTP.
        proveedor: Proveedor cloud afectado.
        url: Endpoint consultado.
        timeout: Ventana de timeout configurada.
        respuesta: Objeto Response, necesario para errores de parsing.

    Returns:
        Una instancia de una subclase de TritonError, ya anotada.

    Raises:
        TypeError: Si el error no es manejado (no debería ocurrir).
    """
    if isinstance(error, httpx.TimeoutException):
        fallo = ProviderTimeoutError(
            "El nodo de telemetría no respondió dentro de la ventana de espera",
            proveedor=proveedor,
            endpoint=url,
            segundos_limite=timeout,
        )
        fallo.add_note("Timeout superado en el nodo de telemetría de respaldo")
        fallo.add_note(f"Proveedor afectado: {proveedor} | Endpoint: {url}")
        fallo.add_note(f"Ventana de espera configurada: {timeout} s")
        fallo.add_note(f"Excepción nativa de transporte: {type(error).__name__}")
        return fallo

    if isinstance(error, httpx.ConnectError):
        fallo = NetworkPeeringError(
            "Pérdida de peering o fallo de resolución DNS del nodo",
            proveedor=proveedor,
            endpoint=url,
            host=httpx.URL(url).host,
        )
        fallo.add_note("El host no resolvió o no hay salida a internet desde el nodo")
        fallo.add_note(f"Proveedor afectado: {proveedor} | Endpoint: {url}")
        return fallo

    if isinstance(error, httpx.HTTPStatusError):
        fallo = CorruptedPayloadError(
            "Estatus HTTP no esperado recibido",
            proveedor=proveedor,
            endpoint=url,
            codigo_estado=error.response.status_code,
            tipo_contenido=error.response.headers.get("content-type"),
        )
        fallo.add_note(
            f"El servidor devolvió {error.response.status_code} "
            f"{error.response.reason_phrase}"
        )
        fallo.add_note(f"Proveedor afectado: {proveedor} | Endpoint: {url}")
        return fallo

    if isinstance(error, json.JSONDecodeError):
        # Necesitamos la respuesta para obtener el status y el content-type
        if respuesta is None:
            raise TypeError("JSONDecodeError requiere el objeto respuesta")
        fallo = CorruptedPayloadError(
            "El nodo respondió con un payload no deserializable como JSON",
            proveedor=proveedor,
            endpoint=url,
            codigo_estado=respuesta.status_code,
            tipo_contenido=respuesta.headers.get("content-type"),
        )
        fallo.add_note("Corrupción de datos detectada en la carga útil del nodo")
        fallo.add_note(f"Content-Type recibido: {respuesta.headers.get('content-type')}")
        fallo.add_note(f"Proveedor afectado: {proveedor} | Endpoint: {url}")
        return fallo

    if isinstance(error, httpx.TransportError):
        # Captura cualquier otro error de transporte no cubierto por
        # TimeoutException o ConnectError (ej. lectura, escritura, proxy).
        fallo = NetworkPeeringError(
            "Fallo de transporte durante el intercambio con el nodo",
            proveedor=proveedor,
            endpoint=url,
            host=httpx.URL(url).host,
        )
        fallo.add_note(f"Excepción nativa de transporte: {type(error).__name__}")
        return fallo

    # Si por algún motivo llega una excepción no contemplada, la relanzamos.
    raise error


async def consultar_proveedor(
    cliente: httpx.AsyncClient,
    proveedor: str,
    url: str,
    *,
    logger: logging.Logger,
    timeout: float,
    resultados: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Interroga de forma asíncrona el nodo de telemetría de un proveedor.

    Traduce todo fallo nativo de ``httpx`` a una excepción semántica de dominio,
    encadenándola con ``raise ... from`` para mantener intacto el ``traceback``
    de la causa raíz original, y le adjunta contexto forense dinámico mediante
    ``add_note()``.

    Args:
        cliente: Cliente HTTP asíncrono compartido por todas las corrutinas.
        proveedor: Identificador del proveedor cloud.
        url: Endpoint real a interrogar.
        logger: Logger de la aplicación.
        timeout: Ventana de espera configurada, en segundos.
        resultados: Acumulador compartido donde se deposita el resultado en caso
            de éxito. Permite al llamador conservar los éxitos parciales aunque
            el ``TaskGroup`` propague un ``ExceptionGroup``.

    Returns:
        Diccionario con el estado operativo del nodo y su latencia medida.

    Raises:
        ProviderTimeoutError: El nodo superó la ventana de espera.
        CorruptedPayloadError: Estatus HTTP no esperado o payload no JSON.
        NetworkPeeringError: Pérdida de peering o fallo de resolución DNS.
    """
    contexto_base = {"proveedor": proveedor, "endpoint": url, "timeout": timeout}
    logger.debug("Iniciando sondeo de telemetría", extra=contexto_base)

    inicio_medicion = time.perf_counter()
    respuesta: httpx.Response | None = None

    try:
        respuesta = await cliente.get(url)
        respuesta.raise_for_status()
        payload = respuesta.json()

    except (httpx.TimeoutException, httpx.ConnectError,
            httpx.HTTPStatusError, httpx.TransportError) as error_nativo:
        fallo = _traducir_error_httpx(error_nativo, proveedor, url, timeout)
        raise fallo from error_nativo

    except json.JSONDecodeError as error_nativo:
        fallo = _traducir_error_httpx(
            error_nativo, proveedor, url, timeout, respuesta=respuesta
        )
        raise fallo from error_nativo

    latencia_ms = round((time.perf_counter() - inicio_medicion) * 1000, 2)

    reporte = {
        "proveedor": proveedor,
        "endpoint": url,
        "codigo_estado": respuesta.status_code,
        "latencia_ms": latencia_ms,
        "estado_operativo": "OPERATIVO",
        "claves_payload": sorted(payload)[:6] if isinstance(payload, dict) else None,
    }

    logger.info(
        "Nodo de telemetría operativo",
        extra={**contexto_base, "latencia_ms": latencia_ms,
               "codigo_estado": respuesta.status_code},
    )

    if resultados is not None:
        resultados.append(reporte)

    return reporte


async def escanear_proveedores(
    proveedores: list[str],
    *,
    timeout: float,
    logger: logging.Logger,
    cluster: str | None = None,
    modo_caos: bool = False,
    resultados: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Orquesta el sondeo paralelo y simultáneo de todos los proveedores.

    Todas las corrutinas de red viven dentro de un mismo bloque
    ``async with asyncio.TaskGroup()``. Si alguna falla, el grupo cancela las
    pendientes y propaga un :class:`ExceptionGroup` que contiene **todos** los
    fallos concurrentes, listo para ser diseccionado con ``except*``.

    Args:
        proveedores: Lista de proveedores a interrogar.
        timeout: Ventana de espera de red, ya sanitizada.
        logger: Logger de la aplicación.
        cluster: Identificador de clúster ya sanitizado, si se proporcionó.
        modo_caos: Activa la tabla de endpoints de inyección de caos.
        resultados: Acumulador compartido de éxitos parciales.

    Returns:
        Lista de reportes de los nodos que respondieron correctamente.

    Raises:
        ExceptionGroup: Agrupa todos los fallos concurrentes de dominio.
    """
    acumulador = [] if resultados is None else resultados

    logger.info(
        "Iniciando barrido de telemetría multicloud",
        extra={
            "proveedores": proveedores,
            "cluster": cluster,
            "timeout": timeout,
            "modo_caos": modo_caos,
        },
    )

    # Almacena las excepciones de dominio capturadas durante el sondeo.
    incidentes: list[TritonError] = []

    async def sondear(proveedor: str, url: str) -> None:
        """Envuelve el sondeo de un nodo preservando la evidencia del incidente.

        El fallo de dominio se recolecta en lugar de propagarse de inmediato.
        Si se re-lanzara aquí, ``TaskGroup`` cancelaría a los nodos hermanos
        todavía en vuelo y sus ``CancelledError`` serían absorbidos por el
        grupo: perderíamos la evidencia de los colapsos *simultáneos*, que es
        justamente lo que el escenario de tormenta de radiación exige auditar.
        """
        try:
            await consultar_proveedor(
                cliente,
                proveedor,
                url,
                logger=logger,
                timeout=timeout,
                resultados=acumulador,
            )
        except TritonError as incidente:
            # Se captura la excepción semántica de dominio, jamás BaseException.
            incidentes.append(incidente)

    limites = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    cabeceras = {"User-Agent": "TritonMonitor/1.0 (+telemetria-multicloud)"}

    async with httpx.AsyncClient(
        timeout=timeout, limits=limites, headers=cabeceras, follow_redirects=True
    ) as cliente:
        async with asyncio.TaskGroup() as grupo:
            for proveedor in proveedores:
                url = resolver_endpoint(proveedor, modo_caos=modo_caos)
                grupo.create_task(
                    sondear(proveedor, url),
                    name=f"telemetria-{proveedor}",
                )

    if incidentes:
        logger.debug(
            "Reconstruyendo el árbol de incidentes concurrentes",
            extra={"incidentes": len(incidentes),
                   "nodos_operativos": len(acumulador)},
        )
        raise ExceptionGroup(
            "Colapso simultáneo de nodos de telemetría multicloud", incidentes
        )

    logger.info(
        "Barrido de telemetría completado sin incidentes",
        extra={"nodos_operativos": len(acumulador)},
    )

    return acumulador
