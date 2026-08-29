"""Utilidades compartidas por las baterías de verificación por módulo.

Centraliza el arranque de las pruebas (resolución de rutas, contadores y
formato de salida) para que cada batería se concentre en verificar su propio
módulo sin repetir andamiaje.
"""

from __future__ import annotations

import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent

if str(RAIZ / "src") not in sys.path:
    sys.path.insert(0, str(RAIZ / "src"))


class Auditoria:
    """Acumula y reporta el resultado de una batería de verificaciones.

    Attributes:
        fallos: Cantidad de verificaciones que no se cumplieron.
    """

    def __init__(self, titulo: str, rol: str) -> None:
        """Imprime el encabezado de la batería.

        Args:
            titulo: Identificación del integrante.
            rol: Rol técnico que se está auditando.
        """
        self.fallos = 0
        print("=" * 74)
        print(f"  VERIFICACIÓN DEL MÓDULO — {titulo}")
        print(f"  {rol}")
        print("=" * 74)

    def check(self, descripcion: str, condicion: bool) -> None:
        """Registra el resultado de una verificación.

        Args:
            descripcion: Qué se está comprobando.
            condicion: Resultado de la comprobación.
        """
        print(f"  [{'OK   ' if condicion else 'FALLA'}] {descripcion}")
        if not condicion:
            self.fallos += 1

    def seccion(self, titulo: str) -> None:
        """Imprime el encabezado de un bloque de pruebas.

        Args:
            titulo: Nombre del bloque.
        """
        print(f"\n{titulo}")

    def cerrar(self) -> int:
        """Imprime el resumen final.

        Returns:
            ``0`` si todo pasó, ``1`` si hubo fallos.
        """
        print("\n" + "=" * 74)
        if self.fallos == 0:
            print("  RESULTADO: todas las verificaciones pasaron.")
        else:
            print(f"  RESULTADO: {self.fallos} verificación/es fallida/s.")
        print("=" * 74)
        return 0 if self.fallos == 0 else 1
