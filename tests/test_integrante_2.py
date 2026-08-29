#!/usr/bin/env python3
"""Batería de verificación del módulo del Integrante 2 (Hernán).

Audita la **concurrencia y la telemetría asíncrona**:

* Mapeo de endpoints reales (nominales y de inyección de caos).
* Traducción de fallos nativos de ``httpx`` a excepciones semánticas, con
  encadenamiento ``raise ... from`` y notas de ``add_note()``.
* Paralelismo efectivo del bloque ``asyncio.TaskGroup``.
* Agrupamiento de colapsos simultáneos en un ``ExceptionGroup``.
* Preservación de los éxitos parciales pese al incidente.

Las peticiones se simulan con ``httpx.MockTransport``, de modo que la batería
es **totalmente local** y determinista: no requiere conexión a internet.

Uso::

    python tests/test_integrante_2.py
"""

from __future__ import annotations

import asyncio
import functools
import logging
import sys
import time

import httpx

from _comun import Auditoria

import triton_telemetry.core as core
from triton_telemetry.core import (
    ENDPOINTS_CAOS,
    ENDPOINTS_NOMINALES,
    PROVEEDORES_SOPORTADOS,
    consultar_proveedor,
    resolver_endpoint,
)
from triton_telemetry.exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
)

logging.disable(logging.CRITICAL)
LOG = logging.getLogger("bateria.muda")


def _transporte(escenario: str) -> httpx.MockTransport:
    """Fabrica un transporte simulado que reproduce un modo de colapso.

    Args:
        escenario: Modo a simular (``timeout``, ``dns``, ``504``, ``422``,
            ``xml`` o cualquier otro valor para una respuesta correcta).

    Returns:
        Transporte simulado listo para inyectar en un ``httpx.AsyncClient``.
    """
    def responder(peticion: httpx.Request) -> httpx.Response:
        if escenario == "timeout":
            raise httpx.ReadTimeout("latencia simulada", request=peticion)
        if escenario == "dns":
            raise httpx.ConnectError("Name or service not known", request=peticion)
        if escenario == "504":
            return httpx.Response(504, text="Gateway Timeout", request=peticion)
        if escenario == "422":
            return httpx.Response(422, text="Unprocessable", request=peticion)
        if escenario == "xml":
            return httpx.Response(200, text="<?xml version='1.0'?><nodo/>",
                                  headers={"content-type": "application/xml"},
                                  request=peticion)
        return httpx.Response(200, json={"userId": 1, "id": 1, "title": "ok"},
                              request=peticion)

    return httpx.MockTransport(responder)


async def _rama(a: Auditoria, escenario: str, esperada: type[Exception],
                nativa: str) -> Exception | None:
    """Verifica una rama de traducción semántica de errores.

    Args:
        a: Auditoría en curso.
        escenario: Modo de colapso a simular.
        esperada: Excepción de dominio que debe emitirse.
        nativa: Nombre de la excepción nativa que debe quedar encadenada.

    Returns:
        La excepción capturada, o ``None`` si no se comportó como se esperaba.
    """
    async with httpx.AsyncClient(transport=_transporte(escenario)) as cliente:
        try:
            await consultar_proveedor(cliente, "AWS", "https://nodo.test/x",
                                      logger=LOG, timeout=1.5)
        except esperada as incidente:
            correcto = (type(incidente) is esperada
                        and incidente.__cause__ is not None
                        and type(incidente.__cause__).__name__ == nativa
                        and len(getattr(incidente, "__notes__", [])) > 0)
            a.check(f"{escenario:8s} -> {esperada.__name__} encadenado desde "
                    f"{nativa} ({len(incidente.__notes__)} notas add_note)", correcto)
            return incidente
        except Exception:  # noqa: BLE001 - la rama incorrecta también es un fallo
            a.check(f"{escenario} -> {esperada.__name__}", False)
    return None


async def _correr(a: Auditoria) -> None:
    """Ejecuta los bloques asíncronos de la batería.

    Args:
        a: Auditoría en curso.
    """
    a.seccion("2.1  MAPEO DE ENDPOINTS REALES")
    a.check("AWS   -> post 1", ENDPOINTS_NOMINALES["AWS"].endswith("/posts/1"))
    a.check("Azure -> post 2", ENDPOINTS_NOMINALES["Azure"].endswith("/posts/2"))
    a.check("GCP   -> post 3", ENDPOINTS_NOMINALES["GCP"].endswith("/posts/3"))
    a.check("caos AWS   -> httpbin delay/3 (gatillo de timeout real)",
            ENDPOINTS_CAOS["AWS"] == "https://httpbin.org/delay/3")
    a.check("caos Azure -> httpbin status/504",
            ENDPOINTS_CAOS["Azure"].endswith("/status/504"))
    a.check("caos GCP   -> httpbin xml (payload corrupto)",
            ENDPOINTS_CAOS["GCP"].endswith("/xml"))
    a.check("resolver_endpoint respeta la bandera modo_caos",
            resolver_endpoint("AWS", modo_caos=True) != resolver_endpoint("AWS"))

    a.seccion("2.2  TRADUCCIÓN SEMÁNTICA DE FALLOS NATIVOS DE httpx")
    await _rama(a, "timeout", ProviderTimeoutError, "ReadTimeout")
    await _rama(a, "dns", NetworkPeeringError, "ConnectError")
    await _rama(a, "504", CorruptedPayloadError, "HTTPStatusError")
    await _rama(a, "422", CorruptedPayloadError, "HTTPStatusError")
    await _rama(a, "xml", CorruptedPayloadError, "JSONDecodeError")

    a.seccion("2.3  TRAZA DE LA CAUSA RAÍZ INTACTA")
    incidente = await _rama(a, "504", CorruptedPayloadError, "HTTPStatusError")
    a.check("el código de estado real quedó capturado",
            incidente is not None and incidente.codigo_estado == 504)
    a.check("__cause__ conserva el objeto response original de httpx",
            incidente is not None
            and incidente.__cause__.response.status_code == 504)

    a.seccion("2.4  ORQUESTACIÓN PARALELA CON asyncio.TaskGroup")

    async def con_demora(peticion: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.4)
        return httpx.Response(200, json={"id": 1}, request=peticion)

    resultados: list[dict] = []
    inicio = time.perf_counter()
    async with httpx.AsyncClient(transport=httpx.MockTransport(con_demora)) as cliente:
        async with asyncio.TaskGroup() as grupo:
            for proveedor in PROVEEDORES_SOPORTADOS:
                grupo.create_task(
                    consultar_proveedor(cliente, proveedor, f"https://n/{proveedor}",
                                        logger=LOG, timeout=5.0,
                                        resultados=resultados),
                    name=f"telemetria-{proveedor}")
    transcurrido = time.perf_counter() - inicio

    a.check(f"3 sondeos de 0.4 s corrieron en paralelo, no en fila "
            f"({transcurrido:.2f} s en total, no 1.2 s)", transcurrido < 0.8)
    a.check("los 3 resultados llegaron al acumulador compartido",
            len(resultados) == 3)

    a.seccion("2.5  AGRUPAMIENTO DE COLAPSOS SIMULTÁNEOS")

    def mixto(peticion: httpx.Request) -> httpx.Response:
        destino = str(peticion.url)
        if destino.endswith("/AWS"):
            raise httpx.ReadTimeout("latencia", request=peticion)
        if destino.endswith("/Azure"):
            raise httpx.ConnectError("Name or service not known", request=peticion)
        if destino.endswith("/GCP"):
            return httpx.Response(504, text="Gateway Timeout", request=peticion)
        return httpx.Response(200, json={"id": 1}, request=peticion)

    cliente_real = httpx.AsyncClient
    endpoint_real = core.resolver_endpoint
    core.httpx.AsyncClient = functools.partial(
        cliente_real, transport=httpx.MockTransport(mixto))
    core.resolver_endpoint = lambda p, modo_caos=False: f"https://nodo.test/{p}"

    try:
        capturados: dict[str, list[str]] = {}
        try:
            await core.escanear_proveedores(["AWS", "Azure", "GCP"], timeout=1.0,
                                            logger=LOG, resultados=[])
            a.check("debía propagar un ExceptionGroup", False)
        except* ProviderTimeoutError as grupo:
            capturados["timeout"] = [e.proveedor for e in grupo.exceptions]
        except* NetworkPeeringError as grupo:
            capturados["peering"] = [e.proveedor for e in grupo.exceptions]
        except* CorruptedPayloadError as grupo:
            capturados["payload"] = [e.proveedor for e in grupo.exceptions]

        a.check(f"except* ProviderTimeoutError  -> {capturados.get('timeout')}",
                capturados.get("timeout") == ["AWS"])
        a.check(f"except* NetworkPeeringError   -> {capturados.get('peering')}",
                capturados.get("peering") == ["Azure"])
        a.check(f"except* CorruptedPayloadError -> {capturados.get('payload')}",
                capturados.get("payload") == ["GCP"])
        a.check("los 3 bloques except* dispararon en la misma corrida",
                len(capturados) == 3)

        a.seccion("2.6  ÉXITOS PARCIALES PRESERVADOS PESE AL INCIDENTE")

        def solo_aws_falla(peticion: httpx.Request) -> httpx.Response:
            if str(peticion.url).endswith("/AWS"):
                raise httpx.ReadTimeout("latencia", request=peticion)
            return httpx.Response(200, json={"id": 1}, request=peticion)

        core.httpx.AsyncClient = functools.partial(
            cliente_real, transport=httpx.MockTransport(solo_aws_falla))
        parciales: list[dict] = []
        try:
            await core.escanear_proveedores(["AWS", "Azure", "GCP"], timeout=1.0,
                                            logger=LOG, resultados=parciales)
        except* ProviderTimeoutError as grupo:
            a.check("1 solo incidente agrupado (AWS)", len(grupo.exceptions) == 1)

        operativos = sorted(r["proveedor"] for r in parciales)
        a.check(f"Azure y GCP siguen en el reporte pese al fallo de AWS: {operativos}",
                operativos == ["Azure", "GCP"])
    finally:
        # PEP 765: solo se restauran los objetos originales, sin control de flujo.
        core.httpx.AsyncClient = cliente_real
        core.resolver_endpoint = endpoint_real


def main() -> int:
    """Ejecuta la batería completa.

    Returns:
        ``0`` si todas las verificaciones pasaron, ``1`` en caso contrario.
    """
    a = Auditoria("INTEGRANTE 2", "Concurrencia y Telemetría Asíncrona")
    asyncio.run(_correr(a))
    return a.cerrar()


if __name__ == "__main__":
    sys.exit(main())
