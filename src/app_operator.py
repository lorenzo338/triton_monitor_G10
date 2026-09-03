#!/usr/bin/env python3
"""``TritonMonitor`` - Punto de entrada oficial de la CLI de telemetría.

Responsable: **Integrante 5 - Coordinador de Integración y Flujo CLI**.

Este script es el orquestador principal del sistema. Su ciclo de vida es:

1. Construir el parser declarativo de ``argparse`` inyectando los ``callables``
   de sanitización del Integrante 1.
2. Inyectar el esquema completo de logging de forma declarativa con
   ``dictConfig`` y arrancar el pipeline no bloqueante del Integrante 4.
3. Lanzar el barrido asíncrono del Integrante 2 dentro de ``asyncio.run``.
4. Diseccionar los fallos concurrentes con bloques ``except*`` quirúrgicos e
   independientes, cerrados por una red de seguridad ``except* TritonError``
   que impide que un incidente de dominio nuevo escape como traceback crudo.
5. Liberar los recursos en un bloque ``finally`` que respeta estrictamente la
   norma **PEP 765**.

Uso::

    python3 src/app_operator.py AWS GCP -c cluster-us-east-01 -t 3.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any

from triton_telemetry import __version__
from triton_telemetry.core import PROVEEDORES_SOPORTADOS, escanear_proveedores
from triton_telemetry.exceptions import (
    CorruptedPayloadError,
    NetworkPeeringError,
    ProviderTimeoutError,
    TritonError,
)
from triton_telemetry.logging_engine import (
    ARCHIVO_LOG_POR_DEFECTO,
    construir_esquema_logging,
    detener_pipeline,
    iniciar_pipeline,
)
from triton_telemetry.sanitizer import (
    validar_identificador_cluster,
    validar_timeout,
)

__all__ = ["DeduplicarProveedores", "construir_parser", "main"]

#: Traducción de los modos operativos de dominio a severidades de consola.
NIVELES_POR_MODO: dict[str, str] = {
    "nominal": "INFO",
    "debug": "DEBUG",
    "emergency": "ERROR",
}

#: Códigos de retorno del proceso.
SALIDA_OK = 0
SALIDA_INCIDENTE = 1
SALIDA_INTERRUPCION = 130


class DeduplicarProveedores(argparse.Action):
    """Colapsa los proveedores repetidos conservando el orden de invocación.

    ``argparse`` valida ``choices`` antes de delegar en la acción, de modo que
    aquí solo llegan proveedores soportados. La deduplicación pertenece a la
    frontera de la CLI y no al núcleo asíncrono: ``core.escanear_proveedores``
    nombra cada corrutina como ``telemetria-<proveedor>``, así que un duplicado
    dispararía dos peticiones HTTP reales al mismo endpoint y produciría dos
    registros con el mismo ``tarea_asyncio``, arruinando la trazabilidad del
    volcado forense.
    """

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str | None = None,
    ) -> None:
        """Deposita en el espacio de nombres la lista ya deduplicada.

        Args:
            parser: Parser que invoca la acción.
            namespace: Espacio de nombres en construcción.
            values: Proveedores crudos, ya validados contra ``choices``.
            option_string: Bandera que originó la acción; ``None`` en los
                argumentos posicionales.
        """
        # dict.fromkeys es el idioma canónico para deduplicar preservando el
        # orden de aparición: los dict conservan el orden de inserción.
        setattr(namespace, self.dest, list(dict.fromkeys(values)))


def construir_parser() -> argparse.ArgumentParser:
    """Construye el parser declarativo de la CLI oficial.

    Integra los validadores personalizados del Integrante 1 en el parámetro
    ``type=``, restringe los modos operativos con ``choices`` y define un grupo
    de opciones mutuamente excluyentes para la verbosidad de la salida de texto.

    Returns:
        Parser configurado, listo para ``parse_args``.
    """
    parser = argparse.ArgumentParser(
        prog="TritonMonitor",
        description=(
            "Monitor CLI oficial de Triton Cloud Services. Interroga en paralelo "
            "los nodos de telemetría de AWS, Azure y GCP mediante peticiones HTTP "
            "asíncronas reales y persiste el resultado en logs JSON estructurados."
        ),
        epilog=(
            "Ejemplo: python3 src/app_operator.py AWS GCP "
            "-c cluster-us-east-01 -t 3.0"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "proveedores",
        nargs="+",
        choices=PROVEEDORES_SOPORTADOS,
        action=DeduplicarProveedores,
        help="Proveedores cloud a interrogar. Los repetidos se colapsan.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"TritonMonitor {__version__}",
        help="Muestra la versión del paquete triton_telemetry y termina.",
    )
    parser.add_argument(
        "-c",
        "--cluster",
        type=validar_identificador_cluster,
        default=None,
        metavar="ID",
        help="Identificador de clúster con formato 'cluster-<region>-<numero>'.",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=validar_timeout,
        default=3.0,
        metavar="SEGUNDOS",
        help="Ventana de espera de red. Rango permitido: 0.1 a 5.0 segundos.",
    )
    parser.add_argument(
        "-m",
        "--modo",
        choices=("nominal", "debug", "emergency"),
        default="nominal",
        help="Modo operativo del monitor.",
    )
    parser.add_argument(
        "--chaos",
        action="store_true",
        help=(
            "Inyecta caos real en caliente: timeout en AWS, estatus 504 en Azure "
            "y payload XML corrupto en GCP."
        ),
    )
    parser.add_argument(
        "--archivo-log",
        default=ARCHIVO_LOG_POR_DEFECTO,
        metavar="RUTA",
        help="Archivo de volcado estructurado JSON.",
    )

    # Grupo de opciones excluyentes de salida de texto: el operador puede pedir
    # más detalle o silenciar la consola, pero nunca ambas cosas a la vez.
    verbosidad = parser.add_mutually_exclusive_group()
    verbosidad.add_argument(
        "-v",
        "--verboso",
        action="store_true",
        help="Fuerza el detalle máximo en la consola.",
    )
    verbosidad.add_argument(
        "-q",
        "--silencioso",
        action="store_true",
        help="Silencia la consola; el volcado JSON en disco se mantiene intacto.",
    )

    return parser


def _resolver_nivel_consola(argumentos: argparse.Namespace) -> str:
    """Determina la severidad de consola combinando modo y verbosidad.

    Args:
        argumentos: Espacio de nombres ya parseado y sanitizado.

    Returns:
        Nombre de la severidad mínima que se mostrará en la consola.
    """
    if argumentos.silencioso:
        return "CRITICAL"
    if argumentos.verboso:
        return "DEBUG"
    return NIVELES_POR_MODO[argumentos.modo]


def _imprimir_reporte(
    resultados: list[dict[str, Any]],
    cluster: str | None,
    *,
    silencioso: bool = False,
) -> None:
    """Vuelca por salida estándar el reporte nominal de los nodos operativos.

    Args:
        resultados: Reportes de los nodos que respondieron correctamente.
        cluster: Identificador de clúster sanitizado, si se proporcionó.
        silencioso: Si es ``True`` se omite toda la salida de texto. El volcado
            JSON en disco no se ve afectado: viaja por el pipeline de logging,
            que es un canal independiente de estos ``print``.
    """
    if silencioso:
        return

    print("\n" + "=" * 72)
    print(f"  REPORTE DE TELEMETRÍA MULTICLOUD - Clúster: {cluster or 'no declarado'}")
    print("=" * 72)

    if not resultados:
        print("  Sin nodos operativos: ningún proveedor devolvió telemetría válida.")
    else:
        print(f"  {'PROVEEDOR':<12}{'ESTADO':<14}{'HTTP':<8}{'LATENCIA REAL':>16}")
        print("  " + "-" * 68)
        for reporte in sorted(resultados, key=lambda item: item["proveedor"]):
            print(
                f"  {reporte['proveedor']:<12}"
                f"{reporte['estado_operativo']:<14}"
                f"{reporte['codigo_estado']:<8}"
                f"{reporte['latencia_ms']:>13.2f} ms"
            )

    print("=" * 72 + "\n")


def _reportar_notas_forenses(
    titulo: str,
    grupo: BaseExceptionGroup,
    *,
    silencioso: bool = False,
) -> None:
    """Imprime en consola las notas forenses de un grupo de excepciones.

    Args:
        titulo: Encabezado del bloque de incidentes.
        grupo: Grupo de excepciones capturado por un bloque ``except*``.
        silencioso: Si es ``True`` se omite la salida por ``stderr``. El
            incidente ya quedó registrado en el volcado estructurado antes de
            llegar aquí, así que no se pierde evidencia forense.
    """
    if silencioso:
        return

    print(f"\n[!] {titulo} ({len(grupo.exceptions)} incidente/s)", file=sys.stderr)

    for indice, incidente in enumerate(grupo.exceptions, start=1):
        print(f"    {indice}. {incidente}", file=sys.stderr)

        for nota in getattr(incidente, "__notes__", None) or []:
            print(f"       · {nota}", file=sys.stderr)

        causa = incidente.__cause__
        if causa is not None:
            print(
                f"       · causa raíz: {type(causa).__name__}: {causa}",
                file=sys.stderr,
            )


def main(argv: list[str] | None = None) -> int:
    """Ejecuta el ciclo de vida completo del monitor.

    Args:
        argv: Argumentos de línea de comandos. Si es ``None`` se toman de
            ``sys.argv``.

    Returns:
        Código de retorno del proceso: ``0`` si todos los nodos respondieron,
        ``1`` si hubo incidentes de red o de payload.
    """
    argumentos = construir_parser().parse_args(argv)

    esquema = construir_esquema_logging(
        nivel_consola=_resolver_nivel_consola(argumentos),
        ruta_log=argumentos.archivo_log,
    )
    listener = iniciar_pipeline(esquema)
    logger = logging.getLogger("triton.operator")

    resultados: list[dict[str, Any]] = []
    codigo_salida = SALIDA_OK
    silencioso = argumentos.silencioso

    try:
        asyncio.run(
            escanear_proveedores(
                argumentos.proveedores,
                timeout=argumentos.timeout,
                logger=logger,
                cluster=argumentos.cluster,
                modo_caos=argumentos.chaos,
                resultados=resultados,
            )
        )

    # -------------------------------------------------------------------------
    # Captura quirúrgica: cada bloque except* atiende una familia de incidentes
    # de forma independiente. Los tres pueden ejecutarse en la misma corrida si
    # el TaskGroup agrupó fallos heterogéneos.
    # PEP 654 prohíbe return, break y continue dentro de un bloque except*: por
    # eso el código de salida se acumula en una variable y se retorna al final.
    # -------------------------------------------------------------------------
    except* ProviderTimeoutError as grupo:
        codigo_salida = SALIDA_INCIDENTE
        logger.error(
            "Colapso por latencia en nodos de telemetría",
            exc_info=grupo,
            extra={"familia_incidente": "timeout",
                   "afectados": len(grupo.exceptions)},
        )
        _reportar_notas_forenses(
            "TIMEOUT DE PROVEEDOR", grupo, silencioso=silencioso
        )

    except* CorruptedPayloadError as grupo:
        # Degradación lógica: un payload corrupto o un estatus HTTP inesperado
        # se mitiga y se registra, pero no detiene el programa.
        logger.warning(
            "Corrupción de payload o estatus HTTP no controlado",
            exc_info=grupo,
            extra={"familia_incidente": "payload",
                   "afectados": len(grupo.exceptions)},
        )
        _reportar_notas_forenses(
            "PAYLOAD CORRUPTO / ESTATUS HTTP", grupo, silencioso=silencioso
        )

    except* NetworkPeeringError as grupo:
        codigo_salida = SALIDA_INCIDENTE
        logger.critical(
            "Pérdida catastrófica de peering o resolución DNS",
            exc_info=grupo,
            extra={"familia_incidente": "peering",
                   "afectados": len(grupo.exceptions)},
        )
        _reportar_notas_forenses(
            "FALLO DE PEERING / DNS", grupo, silencioso=silencioso
        )

    except* TritonError as grupo:
        # Red de seguridad de la integración. Los tres bloques anteriores ya
        # consumieron sus familias, así que aquí solo aterriza un incidente de
        # dominio nuevo que todavía no tiene tratamiento quirúrgico propio. Sin
        # este bloque el subgrupo sobrante se relanzaría al salir del finally y
        # el operador recibiría un traceback crudo, además de perder el código
        # de retorno semántico que acumula esta función.
        codigo_salida = SALIDA_INCIDENTE
        logger.error(
            "Incidente de dominio sin tratamiento quirúrgico específico",
            exc_info=grupo,
            extra={"familia_incidente": "no_clasificado",
                   "afectados": len(grupo.exceptions)},
        )
        _reportar_notas_forenses(
            "INCIDENTE DE DOMINIO NO CLASIFICADO", grupo, silencioso=silencioso
        )

    finally:
        # PEP 765: este bloque no contiene return, break ni continue. Inyectar
        # cualquiera de ellos silenciaría ciegamente una excepción activa y
        # dispararía el SyntaxWarning de Python 3.14.
        _imprimir_reporte(resultados, argumentos.cluster, silencioso=silencioso)
        logger.info(
            "Apagando el pipeline de observabilidad",
            extra={"nodos_operativos": len(resultados),
                   "archivo_log": argumentos.archivo_log},
        )
        detener_pipeline(listener)

    return codigo_salida


if __name__ == "__main__":
    # KeyboardInterrupt se atiende aquí, en una sentencia try independiente y
    # de forma explícita. Nunca mediante `except BaseException`: la jerarquía
    # TritonError cuelga de Exception precisamente para no secuestrar Ctrl+C.
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[i] Monitoreo abortado manualmente por el operador.", file=sys.stderr)
        sys.exit(SALIDA_INTERRUPCION)
