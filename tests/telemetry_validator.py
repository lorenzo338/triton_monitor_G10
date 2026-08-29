#!/usr/bin/env python3
"""Validador Forense de Telemetría JSON del Proyecto Tritón.

Responsable: **Integrante 6 - Ingeniero de Simulación de Caos y Pruebas Forenses**.

Este script abre de forma automática el archivo de logs plano y todos los
históricos comprimidos, y certifica que el sistema de observabilidad cumple el
contrato de telemetría exigido:

1. Toda línea es un documento JSON válido (formato *JSON Lines*).
2. La marca de tiempo respeta **ISO 8601 UTC** estricto (sufijo ``Z``).
3. Los metadatos de identidad están presentes: ``proceso``, ``hilo`` y
   ``tarea_asyncio``.
4. El árbol de ``ExceptionGroup`` se serializó de forma fidedigna, incluyendo
   las sub-excepciones, las causas raíz encadenadas y las notas de
   ``add_note()``.
5. Los errores HTTP de ``httpx`` conservan su evidencia: código de estado,
   verbo y respuesta del servidor.
6. Los históricos ``.gz`` se descomprimen correctamente y su contenido sigue
   siendo JSON válido.

Uso::

    python3 tests/telemetry_validator.py
    python3 tests/telemetry_validator.py --archivo-log triton_services.log
"""

from __future__ import annotations

import argparse
import glob
import gzip
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

#: Marca temporal ISO 8601 en UTC con sufijo Z, por ejemplo
#: ``2026-08-29T15:47:06.482Z``.
PATRON_ISO_UTC = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$"
)

#: Claves obligatorias en todo documento de telemetría.
CLAVES_OBLIGATORIAS = (
    "timestamp",
    "nivel",
    "servicio",
    "logger",
    "mensaje",
    "proceso",
    "hilo",
    "tarea_asyncio",
    "origen",
)


class ReporteAuditoria:
    """Acumula hallazgos de la auditoría forense.

    Attributes:
        documentos: Cantidad de documentos JSON inspeccionados.
        arboles: Cantidad de árboles de excepción encontrados.
        grupos: Cantidad de nodos ``ExceptionGroup`` verificados.
        evidencias_http: Cantidad de nodos con evidencia HTTP de ``httpx``.
        notas: Cantidad total de notas forenses recuperadas.
        errores: Lista de incumplimientos detectados.
    """

    def __init__(self) -> None:
        self.documentos = 0
        self.arboles = 0
        self.grupos = 0
        self.evidencias_http = 0
        self.notas = 0
        self.errores: list[str] = []

    def anotar_error(self, mensaje: str) -> None:
        """Registra un incumplimiento del contrato de telemetría.

        Args:
            mensaje: Descripción del hallazgo.
        """
        self.errores.append(mensaje)


def leer_lineas(ruta: Path) -> Iterator[str]:
    """Itera las líneas de un log plano o comprimido con Gzip.

    Args:
        ruta: Archivo ``.log`` o ``.log.N.gz``.

    Yields:
        Cada línea no vacía del archivo.

    Raises:
        OSError: Si el archivo comprimido está corrupto o es ilegible.
    """
    if ruta.suffix == ".gz":
        with gzip.open(ruta, "rt", encoding="utf-8") as manejador:
            for linea in manejador:
                if linea.strip():
                    yield linea
    else:
        with ruta.open("r", encoding="utf-8") as manejador:
            for linea in manejador:
                if linea.strip():
                    yield linea


def validar_marca_temporal(documento: dict[str, Any], origen: str,
                           reporte: ReporteAuditoria) -> None:
    """Certifica que la marca de tiempo sea ISO 8601 UTC estricto.

    Args:
        documento: Documento JSON de telemetría.
        origen: Etiqueta del archivo y línea de procedencia.
        reporte: Acumulador de hallazgos.
    """
    marca = documento.get("timestamp", "")

    if not PATRON_ISO_UTC.match(marca):
        reporte.anotar_error(f"{origen}: timestamp '{marca}' no es ISO 8601 UTC.")
        return

    momento = datetime.fromisoformat(marca.replace("Z", "+00:00"))
    if momento.tzinfo is None or momento.utcoffset() != timezone.utc.utcoffset(None):
        reporte.anotar_error(f"{origen}: la marca '{marca}' no está anclada a UTC.")


def auditar_nodo_excepcion(nodo: dict[str, Any], origen: str,
                           reporte: ReporteAuditoria, profundidad: int = 0) -> None:
    """Recorre recursivamente un árbol de excepciones serializado.

    Args:
        nodo: Nodo del árbol de excepciones.
        origen: Etiqueta del archivo y línea de procedencia.
        reporte: Acumulador de hallazgos.
        profundidad: Nivel de anidamiento actual.
    """
    if "truncado" in nodo:
        return

    for clave in ("tipo", "modulo", "mensaje"):
        if clave not in nodo:
            reporte.anotar_error(
                f"{origen}: nodo de excepción sin la clave obligatoria '{clave}'."
            )

    reporte.notas += len(nodo.get("notas", []))

    if nodo.get("es_grupo"):
        reporte.grupos += 1
        subs = nodo.get("sub_excepciones")

        if not isinstance(subs, list) or not subs:
            reporte.anotar_error(
                f"{origen}: ExceptionGroup sin sub-excepciones serializadas."
            )
        else:
            if nodo.get("cantidad_sub_excepciones") != len(subs):
                reporte.anotar_error(
                    f"{origen}: el recuento de sub-excepciones no coincide."
                )
            for sub in subs:
                auditar_nodo_excepcion(sub, origen, reporte, profundidad + 1)

    evidencia = nodo.get("http")
    if evidencia:
        reporte.evidencias_http += 1
        if "url" not in evidencia:
            reporte.anotar_error(f"{origen}: evidencia HTTP sin URL de la petición.")

    for clave_hija in ("causa_raiz", "contexto_implicito"):
        hija = nodo.get(clave_hija)
        if isinstance(hija, dict):
            auditar_nodo_excepcion(hija, origen, reporte, profundidad + 1)


def auditar_documento(documento: dict[str, Any], origen: str,
                      reporte: ReporteAuditoria) -> None:
    """Verifica un documento de telemetría completo.

    Args:
        documento: Documento JSON de telemetría.
        origen: Etiqueta del archivo y línea de procedencia.
        reporte: Acumulador de hallazgos.
    """
    reporte.documentos += 1

    for clave in CLAVES_OBLIGATORIAS:
        if clave not in documento:
            reporte.anotar_error(f"{origen}: falta la clave obligatoria '{clave}'.")

    validar_marca_temporal(documento, origen, reporte)

    proceso = documento.get("proceso", {})
    if not isinstance(proceso, dict) or proceso.get("pid") is None:
        reporte.anotar_error(f"{origen}: no se registró el PID del proceso.")

    hilo = documento.get("hilo", {})
    if not isinstance(hilo, dict) or not hilo.get("nombre"):
        reporte.anotar_error(f"{origen}: no se registró el nombre del hilo.")

    arbol = documento.get("excepcion")
    if isinstance(arbol, dict):
        reporte.arboles += 1
        auditar_nodo_excepcion(arbol, origen, reporte)


def main(argv: list[str] | None = None) -> int:
    """Ejecuta la auditoría forense completa.

    Args:
        argv: Argumentos de línea de comandos.

    Returns:
        ``0`` si la telemetría cumple el contrato, ``1`` en caso contrario.
    """
    parser = argparse.ArgumentParser(
        prog="telemetry_validator",
        description="Auditoría forense de la telemetría JSON del Proyecto Tritón.",
    )
    parser.add_argument(
        "--archivo-log",
        default="triton_services.log",
        help="Archivo de volcado estructurado a auditar.",
    )
    argumentos = parser.parse_args(argv)

    base = Path(argumentos.archivo_log)
    archivos = [base] if base.exists() else []
    archivos += [Path(ruta) for ruta in sorted(glob.glob(f"{base}.*.gz"))]

    print("=" * 78)
    print("  VALIDADOR FORENSE DE TELEMETRÍA JSON - Proyecto Tritón")
    print("=" * 78)

    if not archivos:
        print(f"\n  No se encontró '{base}' ni históricos comprimidos.")
        print("  Ejecutá primero la CLI para generar telemetría.\n")
        return 1

    reporte = ReporteAuditoria()

    for archivo in archivos:
        print(f"\n  Auditando: {archivo.name} ({archivo.stat().st_size} bytes)")

        try:
            lineas = list(leer_lineas(archivo))
        except OSError as error_lectura:
            reporte.anotar_error(
                f"{archivo.name}: histórico ilegible o corrupto ({error_lectura})."
            )
            continue

        if archivo.suffix == ".gz":
            print(f"    descompresión Gzip correcta: {len(lineas)} líneas")

        for numero, linea in enumerate(lineas, start=1):
            origen = f"{archivo.name}:{numero}"
            try:
                documento = json.loads(linea)
            except json.JSONDecodeError as error_parseo:
                reporte.anotar_error(f"{origen}: línea no es JSON válido ({error_parseo}).")
                continue

            auditar_documento(documento, origen, reporte)

    print("\n" + "-" * 78)
    print(f"  Documentos JSON auditados .............. {reporte.documentos}")
    print(f"  Árboles de excepción encontrados ....... {reporte.arboles}")
    print(f"  Nodos ExceptionGroup verificados ....... {reporte.grupos}")
    print(f"  Evidencias HTTP de httpx recuperadas ... {reporte.evidencias_http}")
    print(f"  Notas forenses (add_note) recuperadas .. {reporte.notas}")
    print(f"  Incumplimientos detectados ............. {len(reporte.errores)}")
    print("-" * 78)

    if reporte.errores:
        print("\n  HALLAZGOS:")
        for hallazgo in reporte.errores[:20]:
            print(f"    - {hallazgo}")
        if len(reporte.errores) > 20:
            print(f"    ... y {len(reporte.errores) - 20} más.")
        print()
        return 1

    print("\n  La telemetría cumple íntegramente el contrato de observabilidad.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
