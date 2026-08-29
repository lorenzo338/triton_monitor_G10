#!/usr/bin/env python3
"""Batería de verificación del módulo del Integrante 5 (Maximiliano).

Audita el **punto de entrada CLI y la integración general**:

* Parser declarativo de ``argparse`` con validadores inyectados y ``choices``.
* Grupo de opciones mutuamente excluyentes de salida de texto.
* Configuración declarativa del logging mediante ``dictConfig``.
* Captura quirúrgica con bloques ``except*`` independientes (PEP 654).
* Cumplimiento de los HARD GATES, verificado **mecánicamente** recorriendo el
  árbol de sintaxis abstracta (AST) de todos los módulos del proyecto.

La auditoría AST es el aporte diferencial de esta batería: no confía en la
lectura humana del código, sino que analiza su estructura sintáctica real para
demostrar que ninguna prohibición de la cátedra fue violada en ningún archivo.

Todas las pruebas son **locales**: no requieren conexión a internet.

Uso::

    python tests/test_integrante_5.py
"""

from __future__ import annotations

import argparse
import ast
import io
import contextlib
import sys
from pathlib import Path

#: Nodo AST que representa un bloque ``try/except*`` (PEP 654). Python lo expone
#: como ``ast.TryStar`` desde la versión 3.11; se resuelve de forma tolerante por
#: si una versión futura lo unifica con ``ast.Try``.
NODO_TRY_STAR = getattr(ast, "TryStar", None)


def _es_try_star(nodo: ast.AST) -> bool:
    """Indica si un nodo del AST es un bloque ``try/except*``.

    Args:
        nodo: Nodo del árbol de sintaxis abstracta.

    Returns:
        ``True`` si el nodo corresponde a una sentencia ``try/except*``.
    """
    if NODO_TRY_STAR is not None and isinstance(nodo, NODO_TRY_STAR):
        return True
    return isinstance(nodo, ast.Try) and getattr(nodo, "is_star", False)


def _es_try_cualquiera(nodo: ast.AST) -> bool:
    """Indica si un nodo es una sentencia ``try``, con o sin ``except*``.

    Args:
        nodo: Nodo del árbol de sintaxis abstracta.

    Returns:
        ``True`` si el nodo es un ``try`` de cualquier variante.
    """
    return isinstance(nodo, ast.Try) or _es_try_star(nodo)


RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "src"))

from app_operator import (  # noqa: E402
    NIVELES_POR_MODO,
    SALIDA_INCIDENTE,
    SALIDA_OK,
    _resolver_nivel_consola,
    construir_parser,
)
from triton_telemetry.logging_engine import construir_esquema_logging  # noqa: E402

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


def _parsear(*argumentos: str) -> argparse.Namespace | int:
    """Parsea argumentos capturando la salida y el código de aborto.

    Args:
        *argumentos: Argumentos de línea de comandos simulados.

    Returns:
        El espacio de nombres si el parseo tuvo éxito, o el código de salida
        con el que ``argparse`` abortó.
    """
    parser = construir_parser()
    with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
        try:
            return parser.parse_args(list(argumentos))
        except SystemExit as salida:
            return int(salida.code or 0)


def probar_parser() -> None:
    """Verifica el parser declarativo y la sanitización inyectada."""
    seccion("5.1  PARSER DECLARATIVO CON VALIDADORES INYECTADOS")

    ok = _parsear("AWS", "GCP", "-c", "cluster-us-east-01", "-t", "3.0")
    check("acepta una invocación nominal completa", isinstance(ok, argparse.Namespace))
    check(f"proveedores parseados: {getattr(ok, 'proveedores', None)}",
          ok.proveedores == ["AWS", "GCP"])
    check("el timeout llega convertido a float por el validador del Integrante 1",
          isinstance(ok.timeout, float) and ok.timeout == 3.0)

    check("rechaza un proveedor fuera de 'choices' con código 2",
          _parsear("Oracle") == 2)
    check("rechaza un timeout fuera de rango con código 2",
          _parsear("AWS", "-t", "9.5") == 2)
    check("rechaza un identificador de clúster malformado con código 2",
          _parsear("AWS", "-c", "cluster-invalido-id") == 2)
    check("rechaza un modo operativo inexistente con código 2",
          _parsear("AWS", "-m", "paranoico") == 2)

    seccion("5.2  GRUPO DE OPCIONES EXCLUYENTES DE SALIDA DE TEXTO")

    check("-v solo es aceptado", isinstance(_parsear("AWS", "-v"), argparse.Namespace))
    check("-q solo es aceptado", isinstance(_parsear("AWS", "-q"), argparse.Namespace))
    check("-v junto con -q es rechazado con código 2",
          _parsear("AWS", "-v", "-q") == 2)

    seccion("5.3  MODOS OPERATIVOS Y SEVERIDAD DE CONSOLA")

    check("los tres modos de dominio están mapeados",
          set(NIVELES_POR_MODO) == {"nominal", "debug", "emergency"})
    for modo, nivel in (("nominal", "INFO"), ("debug", "DEBUG"), ("emergency", "ERROR")):
        argumentos = _parsear("AWS", "-m", modo)
        check(f"modo '{modo}' -> severidad {nivel}",
              _resolver_nivel_consola(argumentos) == nivel)
    check("-v fuerza DEBUG por encima del modo",
          _resolver_nivel_consola(_parsear("AWS", "-m", "emergency", "-v")) == "DEBUG")
    check("-q silencia la consola sin tocar el volcado en disco",
          _resolver_nivel_consola(_parsear("AWS", "-q")) == "CRITICAL")


def probar_esquema_declarativo() -> None:
    """Verifica que el esquema de logging sea declarativo y completo."""
    seccion("5.4  CONFIGURACIÓN DECLARATIVA CON dictConfig")

    esquema = construir_esquema_logging(nivel_consola="INFO", ruta_log="x.log")

    check("el esquema declara version=1", esquema.get("version") == 1)
    check("declara formatters, handlers y loggers",
          all(clave in esquema for clave in ("formatters", "handlers", "loggers")))
    check("declara el formateador JSON forense",
          "json_forense" in esquema["formatters"])
    check("el logger 'triton' enruta únicamente a la cola",
          esquema["loggers"]["triton"]["handlers"] == ["cola_telemetria"])
    check("el logger 'triton' no propaga al root (sin duplicación de eventos)",
          esquema["loggers"]["triton"]["propagate"] is False)
    check("la cola alimenta consola y archivo rotativo",
          esquema["handlers"]["cola_telemetria"]["handlers"]
          == ["consola", "archivo_rotativo"])
    check("un solo handler de archivo declarado en todo el esquema",
          sum(1 for h in esquema["handlers"].values()
              if "rotativo" in str(h.get("()", ""))) == 1)

    seccion("5.5  CÓDIGOS DE RETORNO DEL PROCESO")
    check(f"salida nominal = {SALIDA_OK}", SALIDA_OK == 0)
    check(f"salida con incidente = {SALIDA_INCIDENTE}", SALIDA_INCIDENTE == 1)


def _modulos_del_proyecto() -> list[Path]:
    """Enumera los archivos fuente auditables del proyecto.

    Returns:
        Lista de rutas a los módulos Python de ``src/``.
    """
    return sorted((RAIZ / "src").rglob("*.py"))


def probar_hard_gates() -> None:
    """Audita mecánicamente los HARD GATES recorriendo el AST de cada módulo."""
    seccion("5.6  AUDITORÍA AST DE LOS HARD GATES (todos los módulos de src/)")

    modulos = _modulos_del_proyecto()
    check(f"módulos fuente detectados para auditar: {len(modulos)}", len(modulos) >= 6)

    capturas_base: list[str] = []
    capturas_desnudas: list[str] = []
    silencios: list[str] = []
    flujo_en_finally: list[str] = []
    flujo_en_except_estrella: list[str] = []
    modulos_con_except_estrella: list[str] = []

    for modulo in modulos:
        arbol = ast.parse(modulo.read_text(encoding="utf-8"), filename=str(modulo))

        for nodo in ast.walk(arbol):
            if isinstance(nodo, ast.ExceptHandler):
                if nodo.type is None:
                    capturas_desnudas.append(f"{modulo.name}:{nodo.lineno}")
                elif isinstance(nodo.type, ast.Name) and nodo.type.id == "BaseException":
                    capturas_base.append(f"{modulo.name}:{nodo.lineno}")

                cuerpo_util = [i for i in nodo.body if not isinstance(i, ast.Pass)]
                if not cuerpo_util:
                    silencios.append(f"{modulo.name}:{nodo.lineno}")

            if _es_try_cualquiera(nodo):
                if _es_try_star(nodo) and nodo.handlers:
                    modulos_con_except_estrella.append(modulo.name)

                for bloque in nodo.finalbody:
                    for interno in ast.walk(bloque):
                        if isinstance(interno, (ast.Return, ast.Break, ast.Continue)):
                            flujo_en_finally.append(f"{modulo.name}:{interno.lineno}")

                if _es_try_star(nodo):
                    for manejador in nodo.handlers:
                        for interno in ast.walk(manejador):
                            if isinstance(interno, (ast.Return, ast.Break, ast.Continue)):
                                flujo_en_except_estrella.append(
                                    f"{modulo.name}:{interno.lineno}"
                                )

    check(f"prohibido capturar BaseException -> {len(capturas_base)} ocurrencias",
          not capturas_base)
    check(f"prohibido el 'except:' desnudo -> {len(capturas_desnudas)} ocurrencias",
          not capturas_desnudas)
    check(f"prohibido silenciar con 'except: pass' -> {len(silencios)} ocurrencias",
          not silencios)
    check(f"PEP 765: sin return/break/continue en 'finally' -> "
          f"{len(flujo_en_finally)} ocurrencias", not flujo_en_finally)
    check(f"PEP 654: sin return/break/continue en 'except*' -> "
          f"{len(flujo_en_except_estrella)} ocurrencias", not flujo_en_except_estrella)
    check(f"se usa 'except*' en: {sorted(set(modulos_con_except_estrella))}",
          "app_operator.py" in modulos_con_except_estrella)

    if capturas_base or capturas_desnudas or flujo_en_finally:
        print("\n  Ubicaciones infractoras detectadas:")
        for ubicacion in capturas_base + capturas_desnudas + flujo_en_finally:
            print(f"    - {ubicacion}")


def probar_captura_quirurgica() -> None:
    """Verifica que los tres bloques except* estén declarados e independientes."""
    seccion("5.7  CAPTURA QUIRÚRGICA: TRES BLOQUES except* INDEPENDIENTES")

    fuente = (RAIZ / "src" / "app_operator.py").read_text(encoding="utf-8")
    arbol = ast.parse(fuente)

    familias: list[str] = []
    tiene_finally = False

    for nodo in ast.walk(arbol):
        if _es_try_star(nodo):
            tiene_finally = bool(nodo.finalbody)
            for manejador in nodo.handlers:
                if isinstance(manejador.type, ast.Name):
                    familias.append(manejador.type.id)

    check(f"familias capturadas: {familias}", len(familias) == 3)
    for esperada in ("ProviderTimeoutError", "CorruptedPayloadError",
                     "NetworkPeeringError"):
        check(f"bloque 'except* {esperada}' declarado", esperada in familias)
    check("el try principal cierra con un bloque finally de limpieza", tiene_finally)


def main() -> int:
    """Ejecuta la batería completa.

    Returns:
        ``0`` si todas las verificaciones pasaron, ``1`` en caso contrario.
    """
    print("=" * 74)
    print("  VERIFICACIÓN DEL MÓDULO — INTEGRANTE 5")
    print("  Coordinador de Integración y Flujo CLI")
    print("=" * 74)

    probar_parser()
    probar_esquema_declarativo()
    probar_hard_gates()
    probar_captura_quirurgica()

    print("\n" + "=" * 74)
    if _fallos == 0:
        print("  RESULTADO: todas las verificaciones pasaron.")
    else:
        print(f"  RESULTADO: {_fallos} verificación/es fallida/s.")
    print("=" * 74)

    return 0 if _fallos == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
