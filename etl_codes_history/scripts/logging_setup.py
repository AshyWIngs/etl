# file: scripts/logging_setup.py
# -*- coding: utf-8 -*-
import logging
import sys
from typing import Optional

class TruncatingFormatter(logging.Formatter):
    """
    Форматтер, который обрезает чрезмерно длинные сообщения на уровнях ≤ INFO, чтобы логи оставались читабельными.
    Обрезка применяется только если `levelno` ≤ `truncate_level`.
    """
    def __init__(
        self,
        fmt: str,
        datefmt: Optional[str] = None,
        max_message_length: Optional[int] = None,
        truncate_level: int = logging.INFO,
        ellipsis: str = "…",
    ) -> None:
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.max_message_length = max_message_length
        self.truncate_level = truncate_level
        self.ellipsis = ellipsis

    def format(self, record: logging.LogRecord) -> str:
        # Собираем исходный текст сообщения (подставляем %-аргументы).
        msg = record.getMessage()

        # Если сообщение слишком длинное и уровень не выше `truncate_level` — укорачиваем.
        if (
            self.max_message_length
            and record.levelno <= self.truncate_level
            and len(msg) > self.max_message_length
        ):
            extra = len(msg) - self.max_message_length
            msg = msg[: self.max_message_length].rstrip() + f"{self.ellipsis} (+{extra} chars)"

        # Подменяем сообщение (возможно уже укороченное), чтобы базовый Formatter использовал его.
        record.message = msg
        return super().format(record)


class PhoenixSqlShortener(logging.Filter):
    """
    Компактирует очень длинные INFO‑сообщения Phoenix SQL:
    - сворачивает перечень колонок в SELECT до `SELECT … FROM`;
    - сохраняет WHERE/params/диапазоны (самое полезное).
    Не трогает записи уровней WARNING/ERROR.
    """

    @staticmethod
    def _find_select_idx(s: str) -> int:
        """Возвращает индекс начала ключевого слова SELECT как отдельного слова, либо -1."""
        sl = s.lower()
        n = len(s)
        pos = 0
        while True:
            i = sl.find('select', pos)
            if i == -1:
                return -1
            left_ok = i == 0 or (not s[i - 1].isalnum() and s[i - 1] != '_')
            right_ok = i + 6 >= n or s[i + 6].isspace()
            if left_ok and right_ok:
                return i
            pos = i + 1

    @staticmethod
    def _skip_quoted(s: str, i: int) -> int:
        """
        Пропускает строковый/идентификаторный литерал, учитывая SQL‑экранирование
        удвоением кавычки. На входе `i` указывает на открывающую кавычку (' или ").
        Возвращает индекс символа, следующего за закрывающей кавычкой, либо len(s),
        если закрытия не найдено (в этом случае считаем остаток строкой).
        Сложность — O(k), где k — длина литерала.
        """
        n = len(s)
        q = s[i]
        i += 1
        while i < n:
            c = s[i]
            if c == q:
                if i + 1 < n and s[i + 1] == q:  # экранированная кавычка
                    i += 2
                    continue
                return i + 1
            i += 1
        return n

    @staticmethod
    def _find_from_idx(s: str, start: int):
        """
        Ищет ключевое слово FROM вне строковых литералов, начиная с позиции `start`.
        Возвращает кортеж (idx_from, whitespace_before), где `idx_from` — индекс 'F' в FROM
        или -1, если не найдено; `whitespace_before` — сохранённый блок пробелов непосредственно
        перед FROM, если он есть (для аккуратного форматирования).

        Переписано в виде простого конечного автомата с выносом логики
        пропуска кавычек в `_skip_quoted` для снижения когнитивной сложности и
        сохранения линейной сложности O(n).
        """
        sl = s.lower()
        n = len(s)
        i = start
        while i < n:
            ch = s[i]

            # 1) Строковые/идентификаторные литералы — пропускаем единым шагом
            if ch == "'" or ch == '"':
                i = PhoenixSqlShortener._skip_quoted(s, i)
                continue

            # 2) Пробелы: сразу проверяем, не начинается ли FROM после них
            if ch.isspace():
                j = i
                while j < n and s[j].isspace():
                    j += 1
                if sl.startswith('from', j):
                    return j, s[i:j]
                i = j
                continue

            # 3) FROM на текущей позиции
            if sl.startswith('from', i):
                return i, ''

            i += 1

        return -1, ''

    @staticmethod
    def _shorten_select(sql: str) -> str:
        """
        Линейно «сворачивает» список колонок между SELECT и FROM.
        Без регулярных выражений — предсказуемая O(n) сложность и отсутствие
        катастрофического/полиномиального бэктрекинга (Sonar S5852).
        Поддержка:
          - пропуск строковых и идентификаторных литералов в кавычках ('...', "...")
            с SQL-экранированием удвоенной кавычкой ('' или "").
          - чувствительность к словным границам у ключевого слова SELECT.
        Возвращает исходную строку, если пара SELECT...FROM не найдена.
        """
        s = sql
        # 1) Найдём корректное слово SELECT
        sel = PhoenixSqlShortener._find_select_idx(s)
        if sel == -1:
            return sql
        after_select = sel + 6  # len('SELECT')

        # 2) Найдём первый FROM вне кавычек
        frm, ws = PhoenixSqlShortener._find_from_idx(s, after_select)
        if frm == -1:
            return sql

        # 3) Свернём список колонок до компактного вида
        prefix = s[:sel]
        select_kw = s[sel:after_select]
        return prefix + select_kw + ' … ' + (ws or '') + s[frm:]

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno <= logging.INFO and record.name == "scripts.db.phoenix_client":
            # Пересчитываем полный текст (с учётом args) и переписываем его.
            msg = record.getMessage()
            # Сворачиваем длинный список колонок в `SELECT … FROM` линейным сканированием.
            msg = self._shorten_select(msg)
            # Сохраняем модифицированное сообщение в record для дальнейшего форматирования.
            record.msg = msg
            record.args = None
        return True

def setup_logging(
    level: str = "INFO",
    *,
    sql_logger_name: str = "scripts.db.phoenix_client",
    sql_level: str = "WARNING",
    max_message_length: Optional[int] = 300,
    datefmt: str = "%Y-%m-%d %H:%M:%S",
) -> None:
    """
    Единый формат логов для всех модулей.
    Примеры:
    2025-08-12 16:47:30 | INFO    | scripts.journal         | message...

    Дополнительно:
    - длинные сообщения на уровне INFO (например, Phoenix SQL с десятками колонок) будут
      компактированы до `SELECT … FROM ...`
    - сверхдлинные сообщения обрезаются до заданной длины с суффиксом `… (+N chars)`
    """
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    handler.setFormatter(
        TruncatingFormatter(
            fmt=fmt,
            datefmt=datefmt,                 # секунды без миллисекунд — чище в консоли
            max_message_length=max_message_length,  # ограничиваем «полотна»
            truncate_level=logging.INFO,
        )
    )
    root = logging.getLogger()
    root.setLevel(lvl)
    # Сжимаем длинные Phoenix SQL сообщения на INFO
    handler.addFilter(PhoenixSqlShortener())
    # очистим хендлеры, чтобы в ноутбуках/демонах не дублировалось
    root.handlers[:] = [handler]
    # По-умолчанию поднимаем уровень детализации для «шумных» модулей (можно вернуть DEBUG при расследованиях)
    sql_lvl = getattr(logging, (sql_level or "WARNING").upper(), logging.WARNING)
    logging.getLogger(sql_logger_name).setLevel(sql_lvl)