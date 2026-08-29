"""El corazón de la observabilidad del Proyecto Tritón.

Este módulo tiene dos responsables claramente separados:

* **Integrante 3 - Ingeniero de Formateo Estructurado JSON**: la clase
  :class:`AsyncJSONFormatter` y el serializador recursivo de ``ExceptionGroup``.
* **Integrante 4 - Ingeniero de Almacenamiento y Desacoplamiento No Bloqueante**:
  la clase :class:`ColaTelemetria`, el handler rotativo acotado, los *callbacks*
  de compresión Gzip y las funciones de arranque y parada del pipeline.

La arquitectura desacopla por completo la emisión del registro de su escritura
física: el hilo del bucle de eventos únicamente encola un ``LogRecord`` en
memoria (operación de microsegundos) y un hilo secundario desatendido se ocupa
de formatear a JSON, escribir en disco, rotar y comprimir.
"""

from __future__ import annotations

import copy
import gzip
import json
import logging
import logging.config
import os
import shutil
import traceback
from datetime import datetime, timezone
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from typing import Any

__all__ = [
    "AsyncJSONFormatter",
    "ColaTelemetria",
    "FormateadorConsola",
    "ARCHIVO_LOG_POR_DEFECTO",
    "TAMANIO_MAXIMO_BYTES",
    "CANTIDAD_RESPALDOS",
    "crear_handler_rotativo",
    "construir_esquema_logging",
    "iniciar_pipeline",
    "detener_pipeline",
    "serializar_excepcion",
]

#: Archivo de volcado estructurado por defecto.
ARCHIVO_LOG_POR_DEFECTO = "triton_services.log"

#: Tamaño máximo del archivo físico antes de disparar el *rollover*: 2 MB.
TAMANIO_MAXIMO_BYTES = 2 * 1024 * 1024

#: Historial estricto de archivos comprimidos conservados en disco.
CANTIDAD_RESPALDOS = 3

#: Profundidad máxima de recursión al expandir árboles de excepciones.
PROFUNDIDAD_MAXIMA = 12

# Conjunto de atributos que Python inyecta de fábrica en todo LogRecord. Todo lo
# que no esté en este conjunto proviene del parámetro `extra=` de las llamadas
# al logger y se publica bajo la clave "contexto" del documento JSON. Se calcula
# a partir de un registro de referencia para no quedar desactualizado entre
# versiones de Python.
_REGISTRO_REFERENCIA = logging.LogRecord("", 0, "", 0, "", None, None)
CAMPOS_ESTANDAR = frozenset(_REGISTRO_REFERENCIA.__dict__) | {
    "message",
    "asctime",
    "taskName",
}


# =============================================================================
# 1. SERIALIZACIÓN RECURSIVA DE EXCEPCIONES  (Integrante 3)
# =============================================================================


def _atributo_seguro(objeto: Any, nombre: str) -> Any:
    """Lee un atributo tolerando que sea una *property* que lance excepciones.

    ``httpx`` implementa ``.request`` como una property que levanta
    ``RuntimeError`` cuando la petición todavía no fue asociada a la excepción.
    Un ``getattr(obj, nombre, None)`` corriente **no** protege contra eso: el
    valor por defecto solo cubre atributos ausentes, no descriptores que
    fallan al evaluarse.

    Un fallo aquí ocurriría dentro del formateador, es decir, perderíamos el
    registro del incidente justo en el momento en que más se lo necesita.

    Args:
        objeto: Objeto a inspeccionar.
        nombre: Nombre del atributo.

    Returns:
        El valor del atributo, o ``None`` si no existe o su lectura falla.
    """
    try:
        return getattr(objeto, nombre, None)
    except Exception:  # noqa: BLE001 - la observabilidad nunca debe tumbar al proceso
        return None


def _extraer_contexto_http(excepcion: BaseException) -> dict[str, Any] | None:
    """Extrae la evidencia HTTP adjunta a una excepción de ``httpx``.

    Se resuelve por *duck typing* sobre los atributos ``request`` y ``response``
    en lugar de acoplar el formateador a la librería concreta: cualquier cliente
    HTTP que exponga esos atributos queda cubierto.

    Args:
        excepcion: Excepción a inspeccionar.

    Returns:
        Diccionario con método, URL, código de estado y un extracto del cuerpo
        del servidor, o ``None`` si la excepción no transporta datos HTTP.
    """
    peticion = _atributo_seguro(excepcion, "request")
    respuesta = _atributo_seguro(excepcion, "response")

    if peticion is None and respuesta is None:
        return None

    evidencia: dict[str, Any] = {}

    if peticion is not None:
        evidencia["metodo"] = _atributo_seguro(peticion, "method")
        evidencia["url"] = str(_atributo_seguro(peticion, "url") or "")

    if respuesta is not None:
        evidencia["codigo_estado"] = _atributo_seguro(respuesta, "status_code")
        evidencia["motivo"] = _atributo_seguro(respuesta, "reason_phrase")

        cabeceras = _atributo_seguro(respuesta, "headers")
        if cabeceras is not None:
            evidencia["content_type"] = cabeceras.get("content-type")

        try:
            cuerpo = getattr(respuesta, "text", "")
        except Exception as error_lectura:  # noqa: BLE001 - se reporta, no se silencia
            evidencia["cuerpo_truncado"] = f"<cuerpo ilegible: {error_lectura!r}>"
        else:
            evidencia["cuerpo_truncado"] = cuerpo[:400]

    return evidencia


def serializar_excepcion(
    excepcion: BaseException | None,
    *,
    profundidad: int = 0,
    visitados: set[int] | None = None,
) -> dict[str, Any] | None:
    """Expande un árbol de excepciones a un nodo JSON jerárquico e indexable.

    Recorre de forma recursiva:

    * las **excepciones secundarias** de un :class:`ExceptionGroup`
      (``exc.exceptions``);
    * las **causas raíz encadenadas** con ``raise ... from ...``
      (``exc.__cause__``);
    * el **contexto implícito** cuando el encadenamiento no fue explícito
      (``exc.__context__``);
    * las **notas forenses dinámicas** adjuntadas con ``add_note()``
      (``exc.__notes__``);
    * la **evidencia HTTP real** de las APIs de ``httpx`` que colapsaron.

    Args:
        excepcion: Raíz del árbol a serializar.
        profundidad: Nivel actual de recursión (uso interno).
        visitados: Identidades ya visitadas, para cortar referencias cíclicas.

    Returns:
        Nodo JSON serializable, o ``None`` si no hay excepción.
    """
    if excepcion is None:
        return None

    visitados = set() if visitados is None else visitados

    if profundidad > PROFUNDIDAD_MAXIMA:
        return {"truncado": "profundidad máxima de recursión alcanzada"}

    if id(excepcion) in visitados:
        return {"truncado": "referencia cíclica de excepción detectada"}

    visitados.add(id(excepcion))

    nodo: dict[str, Any] = {
        "tipo": type(excepcion).__name__,
        "modulo": type(excepcion).__module__,
        "mensaje": str(excepcion),
    }

    # Notas forenses inyectadas dinámicamente en caliente con add_note().
    notas = list(getattr(excepcion, "__notes__", None) or [])
    if notas:
        nodo["notas"] = notas

    # Metadatos de dominio propios de las excepciones de Tritón.
    for atributo in ("proveedor", "endpoint", "codigo_estado", "tipo_contenido",
                     "host", "segundos_limite"):
        valor = _atributo_seguro(excepcion, atributo)
        if valor is not None:
            nodo[atributo] = valor

    evidencia_http = _extraer_contexto_http(excepcion)
    if evidencia_http:
        nodo["http"] = evidencia_http

    if excepcion.__traceback__ is not None:
        nodo["traceback"] = [
            linea.rstrip("\n")
            for linea in traceback.format_tb(excepcion.__traceback__)
        ]

    # Rama 1: grupo de excepciones concurrentes emitido por un TaskGroup.
    if isinstance(excepcion, BaseExceptionGroup):
        nodo["es_grupo"] = True
        nodo["cantidad_sub_excepciones"] = len(excepcion.exceptions)
        nodo["sub_excepciones"] = [
            serializar_excepcion(
                sub, profundidad=profundidad + 1, visitados=visitados
            )
            for sub in excepcion.exceptions
        ]

    # Rama 2: causa raíz encadenada explícitamente con `raise ... from ...`.
    if excepcion.__cause__ is not None:
        nodo["causa_raiz"] = serializar_excepcion(
            excepcion.__cause__, profundidad=profundidad + 1, visitados=visitados
        )
    elif excepcion.__context__ is not None and not excepcion.__suppress_context__:
        nodo["contexto_implicito"] = serializar_excepcion(
            excepcion.__context__, profundidad=profundidad + 1, visitados=visitados
        )

    return nodo


# =============================================================================
# 2. FORMATEADORES  (Integrante 3)
# =============================================================================


class AsyncJSONFormatter(logging.Formatter):
    """Traduce un :class:`logging.LogRecord` nativo a un documento JSON forense.

    Produce una línea JSON por evento (formato *JSON Lines*), directamente
    ingerible por plataformas de agregación como Elasticsearch, Loki o Splunk.

    Cada documento incorpora:

    * ``timestamp`` en **ISO 8601 UTC** estricto, construido con
      ``timezone.utc`` (nunca la hora local del nodo).
    * Identidad de ejecución: ``proceso`` (PID), ``hilo`` (``threadName``) y
      ``tarea_asyncio`` (``taskName``, nativo de Python 3.12+).
    * El bloque ``contexto`` con todo metadato dinámico inyectado a través del
      parámetro ``extra`` de las llamadas al logger.
    * El árbol ``excepcion`` expandido recursivamente por
      :func:`serializar_excepcion`.

    Args:
        indentar: Si es ``True``, genera JSON legible para inspección manual.
            En producción debe permanecer en ``False`` para respetar el formato
            de una línea por evento.
        servicio: Nombre lógico del servicio, embebido en cada documento.
    """

    def __init__(self, *, indentar: bool = False, servicio: str = "triton-monitor") -> None:
        super().__init__()
        self.indentar = indentar
        self.servicio = servicio

    def formatTime(  # noqa: N802 - firma heredada de logging.Formatter
        self, record: logging.LogRecord, datefmt: str | None = None
    ) -> str:
        """Devuelve la marca temporal del registro en ISO 8601 UTC.

        Args:
            record: Registro a datar.
            datefmt: Formato explícito opcional (``strftime``).

        Returns:
            Cadena ISO 8601 con sufijo ``Z``, por ejemplo
            ``2026-08-29T15:47:06.482Z``.
        """
        momento = datetime.fromtimestamp(record.created, tz=timezone.utc)
        if datefmt:
            return momento.strftime(datefmt)
        return momento.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def format(self, record: logging.LogRecord) -> str:
        """Serializa el registro completo a una cadena JSON.

        Args:
            record: Registro emitido por cualquier logger de la aplicación.

        Returns:
            Documento JSON en una sola línea.
        """
        documento: dict[str, Any] = {
            "timestamp": self.formatTime(record),
            "nivel": record.levelname,
            "servicio": self.servicio,
            "logger": record.name,
            "mensaje": record.getMessage(),
            "proceso": {"pid": record.process, "nombre": record.processName},
            "hilo": {"id": record.thread, "nombre": record.threadName},
            "tarea_asyncio": getattr(record, "taskName", None),
            "origen": {
                "modulo": record.module,
                "funcion": record.funcName,
                "linea": record.lineno,
            },
        }

        # Todo atributo que no sea estándar proviene de `extra=` en la llamada.
        contexto = {
            clave: valor
            for clave, valor in record.__dict__.items()
            if clave not in CAMPOS_ESTANDAR
        }
        if contexto:
            documento["contexto"] = contexto

        if record.exc_info and record.exc_info[1] is not None:
            documento["excepcion"] = serializar_excepcion(record.exc_info[1])

        if record.stack_info:
            documento["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(
            documento,
            ensure_ascii=False,
            default=str,
            indent=2 if self.indentar else None,
        )


class FormateadorConsola(logging.Formatter):
    """Formateador legible para el operador humano en la salida estándar.

    El volcado JSON forense es para las máquinas; la consola es para la persona
    que está mirando la terminal durante el incidente.

    Suprime deliberadamente el volcado del ``traceback`` en pantalla. Un
    ``ExceptionGroup`` de tres colapsos concurrentes genera cientos de líneas
    que sepultarían el reporte de notas forenses del operador. El árbol de
    excepciones no se pierde: se persiste íntegro y navegable en el documento
    JSON, que es su lugar natural.
    """

    def __init__(self) -> None:
        super().__init__(
            fmt="[%(asctime)s] [%(levelname)-8s] [%(name)s] -> %(message)s",
            datefmt="%H:%M:%S",
        )

    def formatException(self, ei: Any) -> str:  # noqa: N802 - firma heredada
        """Devuelve una síntesis de una línea en lugar del traceback completo.

        Args:
            ei: Terna ``exc_info`` del registro.

        Returns:
            Resumen del incidente y puntero al volcado estructurado.
        """
        excepcion = ei[1] if ei else None

        if isinstance(excepcion, BaseExceptionGroup):
            return (
                f"          -> {len(excepcion.exceptions)} incidente/s agrupado/s. "
                f"Árbol forense completo en el volcado JSON."
            )
        if excepcion is not None:
            return (
                f"          -> {type(excepcion).__name__}. "
                f"Traza completa en el volcado JSON."
            )
        return ""

    def formatStack(self, stack_info: str) -> str:  # noqa: N802 - firma heredada
        """Omite la pila de llamadas en la salida de consola.

        Args:
            stack_info: Pila capturada por el registro.

        Returns:
            Cadena vacía: la pila se conserva solo en el volcado JSON.
        """
        return ""


# =============================================================================
# 3. PIPELINE NO BLOQUEANTE  (Integrante 4)
# =============================================================================


class ColaTelemetria(QueueHandler):
    """Handler de encolado que preserva la evidencia forense del incidente.

    El :class:`~logging.handlers.QueueHandler` estándar formatea el registro en
    el hilo productor y a continuación **descarta** ``exc_info``, ``args`` y
    ``stack_info`` antes de encolarlo. Ese comportamiento destruiría el árbol de
    ``ExceptionGroup`` que el :class:`AsyncJSONFormatter` necesita expandir en
    el hilo consumidor.

    Esta subclase sobreescribe :meth:`prepare` para trabajar sobre una copia
    superficial del registro, resolver los argumentos de interpolación y
    **mantener intacto** ``exc_info``. El coste sigue siendo de microsegundos:
    no se formatea ni se toca el disco dentro del bucle de eventos.
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Prepara el registro para su viaje entre hilos sin perder evidencia.

        Args:
            record: Registro original emitido en el hilo del bucle de eventos.

        Returns:
            Copia del registro con la interpolación ya resuelta y ``exc_info``
            conservado.
        """
        copia = copy.copy(record)
        copia.msg = record.getMessage()
        copia.args = None
        return copia


def _nombrador_gzip(nombre_por_defecto: str) -> str:
    """Callback ``namer``: añade la extensión ``.gz`` al archivo rotado.

    Args:
        nombre_por_defecto: Nombre que el handler asignaría al respaldo
            (por ejemplo ``triton_services.log.1``).

    Returns:
        El mismo nombre con sufijo ``.gz``.
    """
    return f"{nombre_por_defecto}.gz"


def _rotador_gzip(origen: str, destino: str) -> None:
    """Callback ``rotator``: comprime en caliente el histórico recién cerrado.

    Intercepta el ciclo de volcado (*rollover*), comprime el archivo plano a
    formato ``.gz`` con la biblioteca nativa ``gzip`` y elimina de forma segura
    el residuo sin comprimir del sistema de archivos.

    Args:
        origen: Ruta del archivo plano que acaba de cerrarse.
        destino: Ruta destino ya decorada por :func:`_nombrador_gzip`.
    """
    with open(origen, "rb") as entrada, gzip.open(destino, "wb") as salida:
        shutil.copyfileobj(entrada, salida)
    os.remove(origen)


def crear_handler_rotativo(
    ruta: str = ARCHIVO_LOG_POR_DEFECTO,
    max_bytes: int = TAMANIO_MAXIMO_BYTES,
    backup_count: int = CANTIDAD_RESPALDOS,
    encoding: str = "utf-8",
) -> RotatingFileHandler:
    """Fábrica del manejador rotativo acotado con compresión Gzip.

    Es invocada de forma declarativa por ``dictConfig`` mediante la clave
    ``()``. Limita la escritura física a ``max_bytes`` y mantiene un historial
    estricto de ``backup_count`` archivos comprimidos, evitando la saturación
    del disco del nodo de telemetría.

    Args:
        ruta: Archivo de volcado estructurado.
        max_bytes: Tamaño máximo antes del *rollover*. Por defecto 2 MB.
        backup_count: Cantidad de respaldos ``.gz`` conservados. Por defecto 3.
        encoding: Codificación del archivo de texto.

    Returns:
        Handler rotativo con los *callbacks* de compresión ya instalados.
    """
    handler = RotatingFileHandler(
        filename=ruta,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding=encoding,
        delay=True,
    )
    handler.namer = _nombrador_gzip
    handler.rotator = _rotador_gzip
    return handler


def construir_esquema_logging(
    *,
    nivel_consola: str = "INFO",
    nivel_archivo: str = "DEBUG",
    ruta_log: str = ARCHIVO_LOG_POR_DEFECTO,
) -> dict[str, Any]:
    """Construye el esquema declarativo de logging para ``dictConfig``.

    El esquema declara una única ruta de salida: todos los loggers escriben en
    el handler de cola y ningún otro handler abre el descriptor de archivo. Esto
    satisface la exigencia de no abrir el mismo archivo de logs varias veces de
    forma síncrona y paralela.

    Args:
        nivel_consola: Severidad mínima mostrada al operador humano.
        nivel_archivo: Severidad mínima persistida en el volcado JSON.
        ruta_log: Ruta del archivo de telemetría estructurada.

    Returns:
        Diccionario de configuración compatible con
        :func:`logging.config.dictConfig`.
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json_forense": {
                "()": "triton_telemetry.logging_engine.AsyncJSONFormatter",
                "indentar": False,
                "servicio": "triton-monitor",
            },
            "consola_legible": {
                "()": "triton_telemetry.logging_engine.FormateadorConsola",
            },
        },
        "handlers": {
            # Destino 1: operador humano. No toca el disco.
            "consola": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
                "formatter": "consola_legible",
                "level": nivel_consola,
            },
            # Destino 2: volcado forense JSON, rotado y comprimido.
            "archivo_rotativo": {
                "()": "triton_telemetry.logging_engine.crear_handler_rotativo",
                "formatter": "json_forense",
                "level": nivel_archivo,
                "ruta": ruta_log,
                "max_bytes": TAMANIO_MAXIMO_BYTES,
                "backup_count": CANTIDAD_RESPALDOS,
            },
            # Frontera asíncrona: lo único que ve el bucle de eventos.
            "cola_telemetria": {
                "class": "triton_telemetry.logging_engine.ColaTelemetria",
                "queue": {"()": "queue.Queue", "maxsize": -1},
                "handlers": ["consola", "archivo_rotativo"],
                "respect_handler_level": True,
            },
        },
        "loggers": {
            "triton": {
                "level": "DEBUG",
                "handlers": ["cola_telemetria"],
                "propagate": False,
            },
        },
        "root": {"level": "WARNING", "handlers": ["cola_telemetria"]},
    }


def iniciar_pipeline(esquema: dict[str, Any]) -> QueueListener:
    """Aplica el esquema declarativo y arranca el hilo consumidor de logs.

    Args:
        esquema: Diccionario producido por :func:`construir_esquema_logging`.

    Returns:
        El :class:`~logging.handlers.QueueListener` ya en ejecución, que debe
        detenerse ordenadamente al finalizar el programa.

    Raises:
        RuntimeError: Si ``dictConfig`` no dejó un listener asociado al handler
            de cola (indica un esquema mal formado).
    """
    logging.config.dictConfig(esquema)

    handler_cola = logging.getHandlerByName("cola_telemetria")
    listener = getattr(handler_cola, "listener", None)

    if listener is None:
        raise RuntimeError(
            "El esquema de logging no produjo un QueueListener asociado a la cola."
        )

    listener.start()
    return listener


def detener_pipeline(listener: QueueListener | None) -> None:
    """Drena la cola y apaga ordenadamente el hilo consumidor.

    Garantiza que ningún evento quede atrapado en memoria sin persistir cuando
    la CLI termina, incluso si el programa está saliendo por un incidente.

    Args:
        listener: Listener devuelto por :func:`iniciar_pipeline`. Se acepta
            ``None`` para simplificar los bloques ``finally`` del llamador.
    """
    if listener is not None:
        listener.stop()

    logging.shutdown()
