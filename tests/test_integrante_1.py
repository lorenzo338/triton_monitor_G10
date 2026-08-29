#!/usr/bin/env python3
"""Batería de verificación del módulo del Integrante 1 (Gabriel).

Audita la **robustez de entradas y el diseño de excepciones semánticas**:

* Jerarquía de excepciones colgando de ``Exception``, nunca de ``BaseException``.
* Validador del parámetro ``--timeout`` restringido al rango ``[0.1, 5.0]``.
* Validador del identificador de clúster mediante expresiones regulares.

Todas las pruebas son **locales**: no requieren conexión a internet.

Uso::

    python tests/test_integrante_1.py
"""

from __future__ import annotations

import argparse
import sys

from _comun import Auditoria

from triton_telemetry.exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,
)
from triton_telemetry.sanitizer import (
    PATRON_CLUSTER,
    TIMEOUT_MAXIMO,
    TIMEOUT_MINIMO,
    validar_identificador_cluster,
    validar_timeout,
)


def main() -> int:
    """Ejecuta la batería completa.

    Returns:
        ``0`` si todas las verificaciones pasaron, ``1`` en caso contrario.
    """
    a = Auditoria("INTEGRANTE 1", "Robustez de Entradas y Excepciones")

    a.seccion("1.1  JERARQUÍA DE EXCEPCIONES SEMÁNTICAS")
    a.check("TritonError hereda de Exception", issubclass(TritonError, Exception))
    a.check("TritonError NO hereda directamente de BaseException",
            TritonError.__bases__ == (Exception,))
    for sub in (ProviderTimeoutError, CorruptedPayloadError, NetworkPeeringError):
        a.check(f"{sub.__name__} es subclase de TritonError",
                issubclass(sub, TritonError))
    a.check("Ctrl+C (KeyboardInterrupt) NO queda atrapado por 'except TritonError'",
            not issubclass(KeyboardInterrupt, TritonError))
    a.check("SystemExit NO queda atrapado por 'except TritonError'",
            not issubclass(SystemExit, TritonError))

    a.seccion("1.2  METADATOS DE DOMINIO Y NOTAS FORENSES")
    fallo = ProviderTimeoutError("sin respuesta", proveedor="AWS",
                                 endpoint="https://nodo/x", segundos_limite=1.5)
    a.check("los metadatos de dominio quedan persistidos en la excepción",
            (fallo.proveedor, fallo.segundos_limite) == ("AWS", 1.5))
    a.check("__str__ antepone el proveedor afectado",
            str(fallo) == "[AWS] sin respuesta")
    fallo.add_note("Timeout superado en el nodo de telemetría de respaldo")
    a.check("add_note() adjunta notas forenses dinámicas",
            len(fallo.__notes__) == 1)

    a.seccion(f"1.3  VALIDADOR DE TIMEOUT — rango [{TIMEOUT_MINIMO}, {TIMEOUT_MAXIMO}]")
    for valido in ("0.1", "3.0", "5.0", "2"):
        a.check(f"acepta {valido!r} -> {validar_timeout(valido)}", True)
    for invalido, motivo in (("9.5", "fuera de rango"), ("0.05", "bajo el mínimo"),
                             ("-3", "negativo"), ("abc", "no numérico"),
                             ("", "vacío"), ("nan", "NaN"), ("inf", "infinito")):
        try:
            validar_timeout(invalido)
            a.check(f"rechaza {invalido!r} ({motivo})", False)
        except argparse.ArgumentTypeError:
            a.check(f"rechaza {invalido!r} ({motivo}) con ArgumentTypeError", True)

    a.seccion(f"1.4  VALIDADOR DE CLÚSTER — patrón {PATRON_CLUSTER.pattern}")
    for valido in ("cluster-us-east-01", "cluster-us-west-02", "cluster-eu-central-03"):
        a.check(f"acepta {valido!r}", validar_identificador_cluster(valido) == valido)
    for invalido in ("cluster-invalido-id", "cluster-us-east-1", "CLUSTER-US-EAST-01",
                     "us-east-01", "cluster-us-east-011", "cluster--01",
                     "cluster-us-east-01; rm -rf /"):
        try:
            validar_identificador_cluster(invalido)
            a.check(f"rechaza {invalido!r}", False)
        except argparse.ArgumentTypeError:
            a.check(f"rechaza {invalido!r}", True)

    return a.cerrar()


if __name__ == "__main__":
    sys.exit(main())
