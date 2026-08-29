#!/usr/bin/env python3
"""Batería de verificación del módulo del Integrante 4 (Lorenzo).

Audita el **pipeline de almacenamiento y desacoplamiento no bloqueante**:

* Construcción del ``QueueHandler`` / ``QueueListener`` vía ``dictConfig``.
* Que el hilo emisor no se bloquee al registrar eventos.
* Que ``exc_info`` sobreviva al cruce entre hilos (evidencia forense).
* Rotación acotada a 2 MB con historial estricto de 3 respaldos.
* Compresión Gzip en caliente y eliminación del residuo plano.

Todas las pruebas son **locales**: no requieren conexión a internet.

Uso::

    python tests/test_integrante_4.py
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import queue
import shutil
import sys
import tempfile
import threading
import time
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "src"))

from triton_telemetry.logging_engine import (  # noqa: E402
    CANTIDAD_RESPALDOS,
    TAMANIO_MAXIMO_BYTES,
    ColaTelemetria,
    construir_esquema_logging,
    crear_handler_rotativo,
    detener_pipeline,
    iniciar_pipeline,
)

_fallos = 0


def check(descripcion: str, condicion: bool) -> None:
    """Registra el resultado de una verificación.

    Args:
        descripcion: Qué se está comprobando.
        condicion: Resultado de la comprobación.
    """
    global _fallos
    print(f"  [{'OK   ' if condicion else 'FALLA'}] {descripcion}")
    if not condicion:
        _fallos += 1


def seccion(titulo: str) -> None:
    """Imprime el encabezado de un bloque de pruebas.

    Args:
        titulo: Nombre del bloque.
    """
    print(f"\n{titulo}")


def probar_pipeline() -> None:
    """Verifica el desacoplamiento entre el hilo emisor y la escritura física."""
    seccion("4.1  PIPELINE NO BLOQUEANTE (QueueHandler / QueueListener)")

    check("ColaTelemetria es un QueueHandler", issubclass(ColaTelemetria, QueueHandler))

    esquema = construir_esquema_logging(nivel_consola="CRITICAL", ruta_log="p.log")
    listener = iniciar_pipeline(esquema)
    handler = logging.getHandlerByName("cola_telemetria")

    check("dictConfig construyó la ColaTelemetria",
          type(handler).__name__ == "ColaTelemetria")
    check("la cola es un queue.Queue thread-safe",
          isinstance(handler.queue, queue.Queue))
    check("hay un QueueListener asociado", isinstance(listener, QueueListener))
    check("el listener corre en un HILO SECUNDARIO, no en el principal",
          listener._thread is not None
          and listener._thread is not threading.current_thread())
    check(f"el listener alimenta {len(listener.handlers)} handlers (consola + archivo)",
          len(listener.handlers) == 2)
    check("solo UN handler abre el descriptor del archivo de logs",
          sum(isinstance(h, RotatingFileHandler) for h in listener.handlers) == 1)

    seccion("4.2  EL HILO EMISOR NO SE BLOQUEA")

    registrador = logging.getLogger("triton.carga")
    total = 5000
    inicio = time.perf_counter()
    for indice in range(total):
        registrador.info("evento de carga", extra={"i": indice})
    emision = time.perf_counter() - inicio

    check(f"{total} emisiones en {emision * 1000:.1f} ms "
          f"({emision / total * 1e6:.1f} microsegundos por evento)",
          emision < 2.0)

    detener_pipeline(listener)

    with open("p.log", encoding="utf-8") as manejador:
        persistidos = sum(1 for _ in manejador)

    check(f"la cola se drenó al apagar: {persistidos}/{total} eventos persistidos",
          persistidos == total)


def probar_preservacion_exc_info() -> None:
    """Verifica que la evidencia forense sobreviva al cruce entre hilos."""
    seccion("4.3  PRESERVACIÓN DE exc_info AL CRUZAR DE HILO")

    cola_triton = ColaTelemetria(queue.Queue())
    try:
        raise ValueError("error nativo del transporte")
    except ValueError:
        registro = logging.LogRecord(
            "t", logging.ERROR, "/f.py", 1, "mensaje %s", ("X",), sys.exc_info()
        )
        preparado = cola_triton.prepare(registro)

    check("exc_info SOBREVIVE al pasar por nuestra ColaTelemetria",
          preparado.exc_info is not None
          and preparado.exc_info[1].args == ("error nativo del transporte",))
    check("la interpolación de argumentos ya viene resuelta",
          preparado.getMessage() == "mensaje X" and preparado.args is None)

    cola_estandar = QueueHandler(queue.Queue())
    try:
        raise ValueError("otro error")
    except ValueError:
        registro2 = logging.LogRecord(
            "t", logging.ERROR, "/f.py", 1, "m", None, sys.exc_info()
        )
        check("comparación: el QueueHandler ESTÁNDAR de Python sí lo destruye",
              cola_estandar.prepare(registro2).exc_info is None)


def probar_rotacion_y_gzip() -> None:
    """Verifica la rotación acotada y la compresión Gzip en caliente."""
    seccion("4.4  ROTACIÓN ACOTADA Y COMPRESIÓN GZIP")

    check(f"tamaño máximo configurado = {TAMANIO_MAXIMO_BYTES // 1024 // 1024} MB",
          TAMANIO_MAXIMO_BYTES == 2 * 1024 * 1024)
    check(f"historial estricto de respaldos = {CANTIDAD_RESPALDOS}",
          CANTIDAD_RESPALDOS == 3)

    handler = crear_handler_rotativo(ruta="x.log")
    check("callbacks namer y rotator instalados",
          callable(handler.namer) and callable(handler.rotator))
    check("el namer agrega la extensión .gz", handler.namer("x.log.1") == "x.log.1.gz")
    handler.close()

    # Se baja el umbral para provocar varios rollover sin generar 8 MB de datos.
    esquema = construir_esquema_logging(nivel_consola="CRITICAL", ruta_log="rot.log")
    esquema["handlers"]["archivo_rotativo"]["max_bytes"] = 3000
    listener = iniciar_pipeline(esquema)

    registrador = logging.getLogger("triton.rotacion")
    for indice in range(600):
        registrador.info("carga sostenida", extra={"i": indice, "pad": "z" * 60})

    detener_pipeline(listener)

    comprimidos = sorted(
        nombre for nombre in os.listdir(".")
        if nombre.startswith("rot.log.") and nombre.endswith(".gz")
    )

    check(f"respaldos comprimidos generados: {comprimidos}",
          comprimidos == ["rot.log.1.gz", "rot.log.2.gz", "rot.log.3.gz"])
    check("NO se conservan más de 3 respaldos (disco acotado)",
          len(comprimidos) == 3)
    check("el residuo plano fue eliminado del sistema de archivos",
          not any(os.path.exists(f"rot.log.{n}") for n in (1, 2, 3)))
    check(f"rot.log activo por debajo del umbral "
          f"({os.path.getsize('rot.log')} < 3000 bytes)",
          os.path.getsize("rot.log") < 3000)

    with gzip.open("rot.log.1.gz", "rt", encoding="utf-8") as manejador:
        lineas = manejador.readlines()

    check(f"descompresión Gzip correcta ({len(lineas)} líneas recuperadas)",
          len(lineas) > 0)
    check("el contenido descomprimido sigue siendo JSON válido",
          all(json.loads(linea)["nivel"] == "INFO" for linea in lineas))

    plano = os.path.getsize("rot.log")
    comprimido = os.path.getsize("rot.log.1.gz")
    check(f"compresión efectiva: ~{plano // max(comprimido, 1)}x menos espacio",
          comprimido < plano)


def main() -> int:
    """Ejecuta la batería completa en un directorio temporal aislado.

    Returns:
        ``0`` si todas las verificaciones pasaron, ``1`` en caso contrario.
    """
    print("=" * 74)
    print("  VERIFICACIÓN DEL MÓDULO — INTEGRANTE 4")
    print("  Almacenamiento y Desacoplamiento No Bloqueante")
    print("=" * 74)

    directorio = tempfile.mkdtemp(prefix="triton_int4_")
    original = os.getcwd()
    os.chdir(directorio)

    try:
        probar_pipeline()
        probar_preservacion_exc_info()
        probar_rotacion_y_gzip()
    finally:
        # PEP 765: el bloque de limpieza no inyecta return, break ni continue.
        logging.shutdown()
        os.chdir(original)
        shutil.rmtree(directorio, ignore_errors=True)

    print("\n" + "=" * 74)
    if _fallos == 0:
        print("  RESULTADO: todas las verificaciones pasaron.")
    else:
        print(f"  RESULTADO: {_fallos} verificación/es fallida/s.")
    print("=" * 74)

    return 0 if _fallos == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
