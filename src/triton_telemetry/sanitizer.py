"""Validación declarativa estricta en la frontera de la CLI.

Responsable: Gabriel — Ingeniero de Robustez de Entradas y Excepciones.

Contribución de Gabriel:
    La validación declarativa de esta capa protege al sistema antes de que un
    valor malformado alcance el bucle de eventos, la red o la persistencia. Las
    funciones de este módulo filtran entradas incorrectas, métricas fuera de
    rango y nombres de clúster inválidos, reduciendo la superficie de fallo del
    monitor en escenarios de tormenta o corrupción de datos.

Este módulo intercepta datos corruptos o fuera del rango de dominio *antes* de
que lleguen a tocar el bucle de eventos de ``asyncio`` o los hilos de red. La
estrategia es la validación declarativa: cada función pública de este módulo es
un ``callable`` que se inyecta en el parámetro ``type=`` de ``argparse``.

Cuando una validación falla se levanta :class:`argparse.ArgumentTypeError`, lo
que fuerza a ``argparse`` a imprimir la ayuda formal autogenerada y abortar el
proceso con el código de retorno de sistema ``2`` sin haber abierto un solo
socket.
"""

from __future__ import annotations

import argparse
import re

__author__ = "Gabriel"
__responsable__ = "Gabriel - Ingeniero de Robustez de Entradas y Excepciones"

__all__ = [
    "TIMEOUT_MINIMO",
    "TIMEOUT_MAXIMO",
    "PATRON_CLUSTER",
    "validar_timeout",
    "validar_identificador_cluster",
]

#: Ventana de espera mínima aceptada, en segundos.
TIMEOUT_MINIMO: float = 0.1

#: Ventana de espera máxima aceptada, en segundos.
TIMEOUT_MAXIMO: float = 5.0

#: Patrón obligatorio de los identificadores de clúster: ``cluster-<region>-<numero>``.
#:
#: La región se compone de un código de dos letras y una zona alfabética
#: (``us-east``, ``eu-central``, ``sa-east``), seguida de un ordinal de dos
#: dígitos. Ejemplos válidos: ``cluster-us-east-01``, ``cluster-us-west-02``.
PATRON_CLUSTER: re.Pattern[str] = re.compile(r"^cluster-[a-z]{2}-[a-z]+-\d{2}$")


def validar_timeout(valor: str) -> float:
    """Restringe la ventana de espera de red a un rango flotante seguro.

    Se usa como ``callable`` del parámetro ``--timeout`` de ``argparse``. Un
    timeout demasiado bajo genera falsos positivos de indisponibilidad; uno
    demasiado alto bloquea el monitor desatendido durante minutos.

    Args:
        valor: Cadena cruda entregada por el usuario en la línea de comandos.

    Returns:
        El valor convertido a ``float``, garantizado dentro del rango
        ``[TIMEOUT_MINIMO, TIMEOUT_MAXIMO]``.

    Raises:
        argparse.ArgumentTypeError: Si el valor no es numérico o queda fuera de
            rango. ``argparse`` traduce esta excepción en una salida limpia con
            código de error de sistema ``2``.

    Examples:
        >>> validar_timeout("3.0")
        3.0
    """
    try:
        segundos = float(valor)
    except (TypeError, ValueError) as error_nativo:
        raise argparse.ArgumentTypeError(
            f"El timeout '{valor}' no es un valor numérico válido. "
            f"Se espera un flotante entre {TIMEOUT_MINIMO} y {TIMEOUT_MAXIMO} segundos."
        ) from error_nativo

    # La comparación encadenada descarta también NaN e infinitos: cualquier
    # comparación con NaN es False, por lo que cae en esta misma rama.
    if not TIMEOUT_MINIMO <= segundos <= TIMEOUT_MAXIMO:
        raise argparse.ArgumentTypeError(
            f"El timeout {segundos} está fuera del rango operativo permitido "
            f"[{TIMEOUT_MINIMO}, {TIMEOUT_MAXIMO}] segundos."
        )

    return segundos


def validar_identificador_cluster(valor: str) -> str:
    """Verifica que el identificador de clúster siga el patrón corporativo.

    Se usa como ``callable`` del parámetro opcional ``--cluster``. El formato
    obligatorio es ``cluster-<region>-<numero>``.

    Args:
        valor: Cadena cruda entregada por el usuario en la línea de comandos.

    Returns:
        El identificador normalizado (sin espacios sobrantes en los extremos).

    Raises:
        argparse.ArgumentTypeError: Si el identificador no respeta el patrón.

    Examples:
        >>> validar_identificador_cluster("cluster-us-east-01")
        'cluster-us-east-01'
    """
    identificador = valor.strip()

    if not PATRON_CLUSTER.fullmatch(identificador):
        raise argparse.ArgumentTypeError(
            f"El identificador de clúster '{valor}' no respeta el patrón "
            f"obligatorio 'cluster-<region>-<numero>' "
            f"(ejemplo válido: cluster-us-east-01)."
        )

    return identificador
