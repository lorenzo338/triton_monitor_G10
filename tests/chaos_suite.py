#!/usr/bin/env python3
"""Suite de Simulación de Caos del Proyecto Tritón.

Responsable: **Integrante 6 - Ingeniero de Simulación de Caos y Pruebas Forenses**.

Este script de pruebas automatizado realiza llamadas concurrentes masivas a la
CLI oficial inyectando variables que fuerzan el colapso de red contra las APIs
reales. No verifica el código por dentro: lo ataca desde fuera, como lo haría un
operador hostil, y audita el código de retorno del proceso.

Vectores de ataque implementados
--------------------------------
1. **Colapso de peering**: sobreescribe la URL base de un proveedor apuntándola
   a un host inexistente mediante la variable de entorno
   ``TRITON_ENDPOINT_<PROVEEDOR>``, forzando un ``NetworkPeeringError``.
2. **Colapso por latencia**: reduce la ventana de espera a ``0.1`` segundos
   contra un endpoint de retardo controlado, forzando un ``ProviderTimeoutError``.
3. **Colapso de payload**: activa la bandera ``--chaos``, que inyecta un estatus
   ``504`` en Azure y una respuesta XML corrupta en GCP.
4. **Frontera CLI**: inyecta argumentos malformados para verificar que la
   aplicación aborta con código ``2`` sin abrir un solo socket.

Uso::

    python3 tests/chaos_suite.py
    python3 tests/chaos_suite.py --concurrencia 4
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
CLI = RAIZ_PROYECTO / "src" / "app_operator.py"

#: Códigos de retorno esperados del proceso.
SALIDA_OK = 0
SALIDA_INCIDENTE = 1
SALIDA_ARGUMENTOS = 2


@dataclass(frozen=True)
class CasoDeCaos:
    """Describe un vector de ataque contra la CLI.

    Attributes:
        nombre: Etiqueta legible del escenario.
        argumentos: Argumentos que se pasan a ``app_operator.py``.
        entorno: Variables de entorno inyectadas para desviar los endpoints.
        salidas_aceptadas: Códigos de retorno considerados correctos.
        descripcion: Qué se está validando exactamente.
    """

    nombre: str
    argumentos: list[str]
    salidas_aceptadas: tuple[int, ...]
    descripcion: str
    entorno: dict[str, str] = field(default_factory=dict)


CASOS: list[CasoDeCaos] = [
    CasoDeCaos(
        nombre="A-nominal",
        argumentos=["AWS", "GCP", "-c", "cluster-us-east-01", "-t", "3.0"],
        salidas_aceptadas=(SALIDA_OK, SALIDA_INCIDENTE),
        descripcion="Operación nominal contra las APIs reales de JSONPlaceholder.",
    ),
    CasoDeCaos(
        nombre="B-cluster-malformado",
        argumentos=["AWS", "GCP", "-c", "cluster-invalido-id", "-t", "3.0"],
        salidas_aceptadas=(SALIDA_ARGUMENTOS,),
        descripcion="La frontera CLI debe abortar antes de iniciar el bucle asyncio.",
    ),
    CasoDeCaos(
        nombre="B-timeout-fuera-de-rango",
        argumentos=["AWS", "-t", "9.5"],
        salidas_aceptadas=(SALIDA_ARGUMENTOS,),
        descripcion="El sanitizador debe rechazar timeouts fuera de [0.1, 5.0].",
    ),
    CasoDeCaos(
        nombre="B-proveedor-inexistente",
        argumentos=["Oracle", "-t", "3.0"],
        salidas_aceptadas=(SALIDA_ARGUMENTOS,),
        descripcion="La restricción 'choices' debe rechazar proveedores no soportados.",
    ),
    CasoDeCaos(
        nombre="C-inyeccion-de-caos",
        argumentos=["AWS", "Azure", "GCP", "-c", "cluster-us-west-02", "-t", "1.5",
                    "--chaos"],
        salidas_aceptadas=(SALIDA_OK, SALIDA_INCIDENTE),
        descripcion="Timeout en AWS, 504 en Azure y payload XML corrupto en GCP.",
    ),
    CasoDeCaos(
        nombre="D-colapso-de-peering",
        argumentos=["AWS", "Azure", "-c", "cluster-eu-central-03", "-t", "2.0"],
        entorno={
            "TRITON_ENDPOINT_AWS": "https://nodo-triton-inexistente.invalid/health",
            "TRITON_ENDPOINT_AZURE": "https://otro-nodo-caido.invalid/health",
        },
        salidas_aceptadas=(SALIDA_INCIDENTE,),
        descripcion="Hosts inexistentes: debe emitirse NetworkPeeringError.",
    ),
    CasoDeCaos(
        nombre="D-colapso-por-latencia",
        argumentos=["AWS", "-t", "0.1"],
        entorno={"TRITON_ENDPOINT_AWS": "https://httpbin.org/delay/3"},
        salidas_aceptadas=(SALIDA_INCIDENTE,),
        descripcion="Ventana de espera mínima: debe emitirse ProviderTimeoutError.",
    ),
]


def ejecutar_caso(caso: CasoDeCaos, archivo_log: str) -> tuple[CasoDeCaos, int, str]:
    """Lanza un caso de caos como proceso independiente.

    Args:
        caso: Vector de ataque a ejecutar.
        archivo_log: Ruta del volcado estructurado compartido.

    Returns:
        Tupla con el caso, el código de retorno observado y la salida de error.
    """
    entorno = os.environ.copy()
    entorno.update(caso.entorno)

    proceso = subprocess.run(
        [sys.executable, str(CLI), *caso.argumentos, "--archivo-log", archivo_log],
        capture_output=True,
        text=True,
        timeout=120,
        env=entorno,
        check=False,
    )
    return caso, proceso.returncode, proceso.stderr


def main(argv: list[str] | None = None) -> int:
    """Ejecuta la batería completa de caos y audita los códigos de retorno.

    Args:
        argv: Argumentos de línea de comandos.

    Returns:
        ``0`` si todos los casos se comportaron según lo esperado, ``1`` si no.
    """
    parser = argparse.ArgumentParser(
        prog="chaos_suite",
        description="Suite de simulación de caos contra TritonMonitor.",
    )
    parser.add_argument(
        "--concurrencia",
        type=int,
        default=3,
        help="Cantidad de ataques ejecutados en paralelo.",
    )
    parser.add_argument(
        "--archivo-log",
        default="triton_services.log",
        help="Volcado estructurado compartido por todos los ataques.",
    )
    argumentos = parser.parse_args(argv)

    print("=" * 78)
    print("  SUITE DE SIMULACIÓN DE CAOS - Proyecto Tritón")
    print(f"  Ataques: {len(CASOS)} | Concurrencia: {argumentos.concurrencia}")
    print("=" * 78)

    fallidos = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=argumentos.concurrencia
    ) as ejecutor:
        futuros = [
            ejecutor.submit(ejecutar_caso, caso, argumentos.archivo_log)
            for caso in CASOS
        ]

        for futuro in concurrent.futures.as_completed(futuros):
            caso, codigo, salida_error = futuro.result()
            correcto = codigo in caso.salidas_aceptadas
            marca = "OK  " if correcto else "FALLA"

            if not correcto:
                fallidos += 1

            print(f"\n[{marca}] {caso.nombre}")
            print(f"        {caso.descripcion}")
            print(
                f"        código de retorno: {codigo} "
                f"(esperado: {caso.salidas_aceptadas})"
            )

            if not correcto and salida_error:
                print("        últimas líneas de stderr:")
                for linea in salida_error.strip().splitlines()[-4:]:
                    print(f"          | {linea}")

    print("\n" + "=" * 78)
    print(f"  RESULTADO: {len(CASOS) - fallidos}/{len(CASOS)} ataques con el "
          f"comportamiento esperado")
    print("=" * 78)

    return 0 if fallidos == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
