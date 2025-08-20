# file: scripts/db/clickhouse_client.py
# -*- coding: utf-8 -*-
"""
ClickHouse Native-клиент для ETL (только TCP:9000).

Ключевые идеи
─────────────
• Используем официальную библиотеку `clickhouse-driver` (native протокол).
• Подключение с failover по списку хостов: случайный порядок → первый успешный ответ на `SELECT 1`.
• Транспортное сжатие включено по умолчанию (ZSTD) — экономит трафик и даёт хороший баланс CPU/скорость.
• Безопасное выполнение запросов: защита от «залипания» драйвера в insert-режим после больших вставок.
• INSERT — порциями (чанками). На каждый чанк допускается N повторов (по умолчанию 1) с переподключением.
• Для не-INSERT запросов аккуратно сбрасываем внутренние флаги force_insert* перед исполнением.
• На редкий флап `UnexpectedPacketFromServerError` делаем reconnect() и повтор (1 раз).

Что считать нормой
──────────────────
• Вставки в распределённые (`Distributed`) таблицы приемлемы, но для тяжёлых дедупов чаще лучше:
    INSERT → локальная буферная таблица (ReplicatedMergeTree),
    затем ALTER … REPLACE PARTITION в «чистую» таблицу.
• Для SELECT/DDL используется один общий путь исполнения `_execute_with_retry()` с 1 автоповтором.

Настройки по умолчанию
───────────────────────
• async_insert / wait_for_async_insert — включены, чтобы не блокировать пайплайн.
• insert_distributed_sync=1 — безопаснее для распределённых таблиц.
• network_compression_method=zstd — оптимальный дефолт.
• input_format_null_as_default=0 — не подменяем NULL.
• use_client_time_zone=1 — не пересчитываем TZ на сервере.

Диагностика
───────────
• При старте логируем удачный хост (host:port, db).
• При неудаче подключения к конкретному хосту — предупреждение и переход к следующему.
• При флапе UnexpectedPacketFromServerError — предупреждение и один повтор.

Пример использования
────────────────────
>>> ch = ClickHouseClient(hosts=["10.0.0.1","10.0.0.2"], database="stg")
>>> ch.execute("CREATE TABLE IF NOT EXISTS stg.t (x UInt32) ENGINE = TinyLog")
>>> ch.insert_rows("stg.t", [(1,), (2,), (3,)], ["x"])
>>> v = ch.query_scalar("SELECT sum(x) FROM stg.t")
>>> assert v == 6
>>> ch.close()

Примечание
──────────
Этот модуль не знает про схему ваших таблиц — он лишь безопасно и стабильно ходит в ClickHouse.
"""
import logging
import random
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence

from clickhouse_driver import Client as NativeClient  # type: ignore[reportMissingImports]
import re
from clickhouse_driver import errors as ch_errors  # type: ignore[reportMissingImports]


# Хелпер для безопасного SQL-литерала (ClickHouse)
def _quote_literal(value: str) -> str:
    """
    Безопасно оборачивает строку в одинарные кавычки для ClickHouse.
    Используем SQL-совместимое экранирование путём удвоения одинарных кавычек.
    """
    s = "" if value is None else str(value)
    return "'" + s.replace("'", "''") + "'"

log = logging.getLogger("scripts.db.clickhouse_client")

# Регулярка для безопасной проверки «это INSERT?» с учётом пробелов и комментариев.
# Нужна из‑за известной особенности clickhouse-driver: если оставить соединение
# в режиме "force insert" (после большой вставки), то следующий не‑INSERT запрос
# может быть ошибочно обработан как вставка. Мы явно проверяем начало текста запроса
# и для не‑INSERT запросов сбрасываем внутренние флаги (см. _clear_force_insert()).

_INSERT_RE = re.compile(
    r"^\s*(?:--.*?$|\s|/\*.*?\*/)*INSERT\b",
    re.IGNORECASE | re.DOTALL | re.MULTILINE,
)

# Подавляет «портянки» от внутреннего логгера clickhouse-driver при неудачных попытках подключения
def _configure_driver_logging() -> None:
    """
    Подавляет «портянки» от внутреннего логгера clickhouse-driver при неудачных попытках
    подключения (он пишет WARNING с exc_info и stacktrace на каждый хост).

    Поведение:
    • По умолчанию (если не включён DEBUG/ETL_TRACE_EXC) — опускаем уровень до ERROR,
      чтобы WARNING с трассировкой не попадали в общий лог.
    • Можно переопределить через переменную окружения CH_DRIVER_LOG_LEVEL:
        - OFF    — полностью «выключить» (ставим уровень выше CRITICAL)
        - ERROR  — по умолчанию (скрывает WARNING)
        - WARNING/INFO/DEBUG — показать больше.
    • Если установлен ETL_TRACE_EXC=1 или запущено с --log-level=DEBUG — ничего не подавляем.
    """
    try:
        trace_on = str(os.getenv("ETL_TRACE_EXC", "")).strip().lower() in ("1", "true", "yes", "on")
        if trace_on or logging.getLogger().isEnabledFor(logging.DEBUG):
            return  # подробный режим, оставляем дефолтные уровни драйвера

        level_name = str(os.getenv("CH_DRIVER_LOG_LEVEL", "ERROR")).strip().upper()
        if level_name == "OFF":
            target_level = logging.CRITICAL + 10  # фактически отключаем
        else:
            target_level = getattr(logging, level_name, logging.ERROR)

        for lname in ("clickhouse_driver.connection", "clickhouse_driver.client", "clickhouse_driver.pool"):
            lg = logging.getLogger(lname)
            lg.setLevel(target_level)
    except Exception:
        # Безопасность прежде всего: любые сбои конфигурации логгера — не мешают ходу программы.
        pass

class ClickHouseClient:
    """
    Тонкая обёртка над `clickhouse-driver` с:
      • failover-подключением по списку хостов,
      • безопасным выполнением не‑INSERT запросов (сброс force_insert*),
      • единообразным автоповтором при UnexpectedPacketFromServerError,
      • порционной вставкой с ограниченным числом ретраев и переподключением.

    Объект создаёт соединение при инициализации. При необходимости вызывайте `reconnect()`.
    """
    def __init__(
        self,
        hosts: List[str],
        port: int = 9000,
        database: str = "default",
        user: str = "default",
        password: str = "",
        connect_timeout: int = 5,
        send_receive_timeout: int = 600,
        compression: bool = True,       # включает сжатие транспорта; нужны lz4/zstd (cityhash обязателен)
        settings: Optional[Dict[str, Any]] = None,
        insert_chunk_size: Optional[int] = None,   # размер чанка вставки (строк)
        insert_max_retries: Optional[int] = None,  # повторов на чанк при ошибке соединения
        cluster: Optional[str] = None,  # опционально, используется только для вспомогательных проверок
    ):
        self.hosts = [h.strip() for h in (hosts or []) if h and h.strip()]
        if not self.hosts:
            raise ValueError("CH_HOSTS пуст — укажи хотя бы один хост.")
        self.port = int(port)
        self.db = database
        self.user = user
        self.password = password
        self.connect_timeout = int(connect_timeout)
        self.send_receive_timeout = int(send_receive_timeout)
        self.compression = bool(compression)

        # Необязательное имя кластера, используется только для вспомогательных проверок
        self.cluster = cluster

        # Базовые настройки клиента. Их можно переопределить через аргумент `settings`.
        # Обоснование значений см. в модульном докстринге.
        self.settings: Dict[str, Any] = {
            "async_insert": 1,
            "wait_for_async_insert": 1,
            "insert_distributed_sync": 1,
            # сетевое сжатие — ZSTD даёт лучший баланс CPU/трафик
            "network_compression_method": "zstd",
            # не подменять NULL значениями по умолчанию
            "input_format_null_as_default": 0,
            # не пересчитывать TZ на сервере
            "use_client_time_zone": 1,
        }
        if settings:
            self.settings.update(settings)
        # Определяем insert_chunk_size и insert_max_retries (ленивый импорт конфига + запасные дефолты)
        cfg = None
        if insert_chunk_size is None or insert_max_retries is None:
            try:
                from scripts.config import Settings  # late import чтобы не тянуть конфиг на уровне модуля
                cfg = Settings()
            except Exception:
                cfg = None

        if insert_chunk_size is None:
            insert_chunk_size = int(getattr(cfg, "CH_INSERT_BATCH", 20000)) if cfg else 20000
        if insert_max_retries is None:
            insert_max_retries = int(getattr(cfg, "CH_INSERT_MAX_RETRIES", 1)) if cfg else 1

        self.insert_chunk_size = int(insert_chunk_size) if int(insert_chunk_size) > 0 else 20000
        self.insert_max_retries = int(insert_max_retries)
        self.client: Optional[NativeClient] = None
        self.current_host: Optional[str] = None
        _configure_driver_logging()
        self._connect_any()

    def _is_insert(self, sql: str) -> bool:
        """
        Возвращает True, если запрос начинается с ключевого слова INSERT
        (игнорируя ведущие пробелы и строки комментариев '--' и '/* ... */').
        """
        return bool(_INSERT_RE.match(sql or ""))

    def _clear_force_insert(self) -> None:
        """
        Сбрасывает внутренние флаги драйвера `force_insert_query` / `force_insert`, если они выставлены.
        Это предотвращает редкий сценарий, когда после INSERT следующий SELECT/DDL воспринимается как вставка.
        Безопасно вызывается перед любым не‑INSERT запросом (best-effort, ошибки игнорируются).
        """
        try:
            conn = getattr(self.client, "connection", None)
            if conn is not None:
                if hasattr(conn, "force_insert_query"):
                    conn.force_insert_query = False
                if hasattr(conn, "force_insert"):
                    conn.force_insert = False
        except Exception:
            # best-effort: не мешаем основному потоку
            pass

    def _cli(self) -> NativeClient:
        """
        Возвращает активный клиент ClickHouse.
        Нужен для того, чтобы явным образом показать анализатору типов, что self.client не None.
        """
        c = self.client
        if c is None:
            raise RuntimeError("ClickHouse client не инициализирован.")
        return c

    def _execute_once(
        self,
        sql: str,
        params: Optional[Sequence] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        """
        Единичное выполнение `self.client.execute(...)` с учётом режима запроса:
          • для не‑INSERT: предварительно сбрасываем force_insert*, и если `params` пуст,
            не передаём его вовсе (иначе драйвер может переключиться на ветку вставки);
          • для INSERT: выполняем как обычно.
        Возвращает результат, как его возвращает `clickhouse-driver`.
        """
        is_insert = self._is_insert(sql)
        exec_settings = settings or {}
        if not is_insert:
            self._clear_force_insert()

        # ВАЖНО: если params пустой — вообще не передавать его, чтобы драйвер не переключился на ветку вставки
        if params is None or (isinstance(params, (list, tuple)) and len(params) == 0):
            return self._cli().execute(sql, settings=exec_settings)
        return self._cli().execute(sql, params=params, settings=exec_settings)

    def _execute_with_retry(
        self,
        sql: str,
        params: Optional[Sequence] = None,
        settings: Optional[Dict[str, Any]] = None,
    ):
        """
        Выполнение запроса с одним авто‑повтором на `UnexpectedPacketFromServerError`:
          1) пробуем _execute_once;
          2) при исключении — логируем предупреждение, делаем `reconnect()` и повторяем ещё раз.
        Полезно после крупных INSERT или при обрыве соединения сервером.
        """
        try:
            return self._execute_once(sql, params=params, settings=settings)
        except ch_errors.UnexpectedPacketFromServerError as e:
            # типичный флап: сервер закрыл соединение, а клиент ждал "sample block/Data"
            log.warning(
                "UnexpectedPacketFromServerError на '%s': %s — reconnect() и один повтор.",
                (sql.split()[0] if sql else "query"), e
            )
            self.reconnect()
            return self._execute_once(sql, params=params, settings=settings)

    def exists_table(self, table: str, database: Optional[str] = None) -> bool:
        """
        Локальная проверка существования таблицы на текущем хосте.
        Принимает как FQN ('db.tbl'), так и имя в текущей БД.
        """
        db = database or self.db
        fqn = table if "." in table else f"{db}.{table}"
        try:
            val = self.query_scalar(f"EXISTS TABLE {fqn}")
            return bool(val)
        except Exception as e:
            log.warning("EXISTS TABLE %s: ошибка проверки: %s", fqn, e)
            return False

    def tables_exist_local(self, db: Optional[str], tables: Sequence[str]) -> Dict[str, bool]:
        """
        Возвращает карту наличия таблиц на ТЕКУЩЕМ хосте (без кластерной магии).
        Ключи — FQN ('db.tbl').
        """
        database = db or self.db
        result: Dict[str, bool] = {}
        for t in tables:
            fqn = t if "." in t else f"{database}.{t}"
            result[fqn] = self.exists_table(fqn, database)
        return result

    def tables_exist_cluster(self, cluster: str, db: Optional[str], tables: Sequence[str]) -> Dict[str, bool]:
        """
        Кластерная проверка наличия таблиц: считаем записи в system.tables на всех репликах кластера.
        Возвращает карту FQN → True/False.
        """
        database = db or self.db
        result: Dict[str, bool] = {}
        cl = _quote_literal(cluster)
        for t in tables:
            name = t.split(".")[-1]
            sql = (
                "SELECT count() "
                f"FROM clusterAllReplicas({cl}, system.tables) "
                f"WHERE database = {_quote_literal(database)} AND name = {_quote_literal(name)}"
            )
            try:
                cnt = int(self.query_scalar(sql) or 0)
                fqn = f"{database}.{name}"
                result[fqn] = cnt > 0
            except Exception as e:
                fqn = f"{database}.{name}"
                log.warning("Проверка таблицы в кластере '%s' для %s: ошибка: %s", cluster, fqn, e)
                result[fqn] = False
        return result

    def probe_hosts_for_tables(self, db: Optional[str], tables: Sequence[str]) -> Dict[str, Dict[str, bool]]:
        """
        Обходит все CH_HOSTS и возвращает карту: host → {FQN → exists(bool)}.
        Проверка выполняется ЛОКАЛЬНО на каждом хосте через временный клиент.
        """
        database = db or self.db
        report: Dict[str, Dict[str, bool]] = {}
        for host in self.hosts:
            temp: Optional[NativeClient] = None
            host_map: Dict[str, bool] = {}
            try:
                temp_cli = NativeClient(
                    host=host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=database,
                    client_name="etl_codes_history_probe",
                    connect_timeout=self.connect_timeout,
                    send_receive_timeout=self.send_receive_timeout,
                    compression=self.compression,
                    settings=self.settings,
                )
                temp = temp_cli
                temp_cli.execute("SELECT 1")
                for t in tables:
                    fqn = t if "." in t else f"{database}.{t}"
                    try:
                        val = temp_cli.execute(f"EXISTS TABLE {fqn}")
                        ok = bool(val and val[0][0])
                    except Exception as e:
                        log.warning("EXISTS TABLE %s@%s: ошибка: %s", fqn, host, e)
                        ok = False
                    host_map[fqn] = ok
            except Exception as e:
                log.warning("Не удалось выполнить probe на хосте %s: %s", host, e)
                for t in tables:
                    fqn = t if "." in t else f"{database}.{t}"
                    host_map[fqn] = False
            finally:
                try:
                    if temp is not None:
                        temp.disconnect()
                except Exception:
                    pass
            report[host] = host_map
        return report

    def switch_to_host(self, host: str) -> None:
        """
        Принудительное переключение на указанный хост (без перемешивания).
        Поднимает исключение, если подключение не удалось.
        """
        try:
            if self.client:
                self.client.disconnect()
        except Exception:
            pass
        cli = NativeClient(
            host=host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.db,
            client_name="etl_codes_history",
            connect_timeout=self.connect_timeout,
            send_receive_timeout=self.send_receive_timeout,
            compression=self.compression,
            settings=self.settings,
        )
        cli.execute("SELECT 1")
        self.client = cli
        self.current_host = host
        log.info("Переключился на ClickHouse %s:%d, db=%s", host, self.port, self.db)

    def ensure_tables(self, tables: Sequence[str], db: Optional[str] = None, cluster: Optional[str] = None, try_switch_host: bool = True) -> None:
        """
        Гарантирует наличие необходимых таблиц.
        Алгоритм:
          1) Если задан cluster — проверяем наличие на уровне кластера (clusterAllReplicas).
             Если всё ок — выходим.
          2) Иначе/дополнительно — проверяем локально на текущем хосте.
             Если отсутствуют и try_switch_host=True — пробуем найти хост, где все таблицы есть, и переключаемся.
             Если таковой не найден — поднимаем RuntimeError с детальным отчётом.
        """
        database = db or self.db
        names = [t if "." in t else f"{database}.{t}" for t in tables]

        # 1) Кластерная проверка (опционально)
        if cluster:
            cluster_map = self.tables_exist_cluster(cluster, database, tables)
            missing_cluster = [n for n in names if not cluster_map.get(n, False)]
            if not missing_cluster:
                return  # на кластере всё видно

        # 2) Локальная проверка на текущем хосте
        local_map = self.tables_exist_local(database, tables)
        missing_local = [n for n in names if not local_map.get(n, False)]
        if not missing_local:
            return

        if not try_switch_host:
            raise RuntimeError(f"Missing tables on host {self.current_host or '?'}: {', '.join(missing_local)}")

        # 3) Попробуем найти подходящий хост
        probe = self.probe_hosts_for_tables(database, tables)
        candidates = []
        for host, tblmap in probe.items():
            if all(tblmap.get(n, False) for n in names):
                candidates.append(host)

        if candidates:
            # Берём первый подходящий хост и переключаемся
            self.switch_to_host(candidates[0])
            return

        # 4) Никто не подошёл — собираем сводку и падаем
        lines = [f"clickhouse ensure_tables failed: ни на одном хосте не найден полный набор таблиц."]
        for host, tblmap in probe.items():
            miss = [n for n in names if not tblmap.get(n, False)]
            lines.append(f"  - {host}: missing={miss if miss else 'OK'}")
        raise RuntimeError("\n".join(lines))

    def _connect_any(self) -> None:
        """
        Подключается к первому «живому» хосту из списка:
          • перемешиваем хосты для равномерного распределения,
          • пробуем создать NativeClient и выполнить `SELECT 1`,
          • при успехе — фиксируем `current_host` и выходим,
          • при неудаче — предупреждение в лог и пробуем следующий.
        В случае полного провала возбуждает последнее перехваченное исключение.
        """
        last_err: Optional[Exception] = None
        hosts = self.hosts[:]
        random.shuffle(hosts)
        for host in hosts:
            try:
                cli = NativeClient(
                    host=host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.db,
                    client_name="etl_codes_history",
                    connect_timeout=self.connect_timeout,
                    send_receive_timeout=self.send_receive_timeout,
                    compression=self.compression,
                    settings=self.settings,
                )
                cli.execute("SELECT 1")
                self.client = cli
                self.current_host = host
                log.info("Подключен к ClickHouse (native) %s:%d, db=%s", host, self.port, self.db)
                return
            except Exception as e:
                last_err = e
                log.warning("Не удалось подключиться к ClickHouse на %s:%d: %s", host, self.port, e)
                self.client = None
                continue
        raise last_err or RuntimeError("Не удалось подключиться к ClickHouse ни к одному хосту.")

    def _iter_chunks(self, rows: Iterable[Any]) -> Iterable[List[Any]]:
        """
        Генератор, разбивающий входной iterable `rows` на списки длиной до `insert_chunk_size`.
        Используем списки (а не кортежи), потому что драйверу удобнее подавать списки батчей.
        """
        chunk: List[Any] = []
        for row in rows:
            chunk.append(row)
            if len(chunk) >= self.insert_chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def close(self):
        """
        Корректно закрывает соединение с ClickHouse. Любые ошибки при disconnect игнорируются.
        """
        try:
            if self.client:
                self.client.disconnect()
        except Exception:
            pass

    def query_scalar(self, sql: str) -> Any:
        """
        Выполняет SELECT и возвращает первое скалярное значение первой строки.
        Использует безопасный путь исполнения + 1 авто‑повтор при UnexpectedPacketFromServerError.
        Подходит для EXISTS/COUNT/проверочных запросов.
        """
        res = self._execute_with_retry(sql)
        return res[0][0] if res else None

    def execute(self, sql: str, params: Optional[Sequence] = None, settings: Optional[Dict[str, Any]] = None) -> None:
        """
        Универсальный вызов для DDL/DML/ALTER/INSERT SELECT с поддержкой `settings`.
        • Защищён от «залипания» драйвера в insert‑режим на не‑INSERT запросах.
        • Делает один авто‑повтор при UnexpectedPacketFromServerError.
        Ничего не возвращает — как и стандартный `Client.execute` для не‑SELECT.
        """
        self._execute_with_retry(sql, params=params, settings=settings)

    def query_all(self, sql: str, params: Optional[Sequence] = None, settings: Optional[Dict[str, Any]] = None) -> List[tuple]:
        """
        Выполняет SELECT и возвращает список строк (list[tuple]).
        На вход можно передавать `params` и `settings`. Для не‑INSERT запросов перед вызовом
        сбрасываются force_insert* флаги; при флапе есть один авто‑повтор.
        """
        return self._execute_with_retry(sql, params=params, settings=settings)  # type: ignore[return-value]

    def reconnect(self) -> None:
        """
        Принудительно разрывает текущее соединение (если есть) и заново пытается подключиться
        к одному из доступных хостов (_connect_any). Используется в ретраях и для ручного восстановления.
        """
        try:
            if self.client:
                self.client.disconnect()
        except Exception:
            pass
        self._connect_any()

    def insert_rows(self, table: str, rows: Iterable[Any], columns: Sequence[str]) -> int:
        """
        Вставка данных в таблицу порциями (чанками).
        Параметры:
          • table   — FQN таблицы ('db.tbl') или имя в текущей БД.
          • rows    — iterable с уже подготовленными записями (tuple/list) в порядке `columns`.
          • columns — последовательность имён колонок.

        Поведение:
          • Строится SQL вида `INSERT INTO db.tbl (c1, c2, ...) VALUES`.
          • Поток `rows` режется на чанки по `insert_chunk_size`.
          • На каждый чанк допускается до `insert_max_retries` попыток; между попытками — переподключение.
          • При превышении лимита ретраев — исключение пробрасывается вызывающему коду.

        Возвращает количество реально вставленных строк.
        """
        table_fqn = table if "." in table else f"{self.db}.{table}"
        sql = f"INSERT INTO {table_fqn} ({', '.join(columns)}) VALUES"

        total = 0
        for chunk in self._iter_chunks(rows):
            attempt = 0
            while True:
                try:
                    self._cli().execute(sql, chunk, types_check=True)
                    total += len(chunk)
                    break
                except Exception as e:
                    attempt += 1
                    if attempt > self.insert_max_retries:
                        # Пробрасываем последнюю ошибку, чтобы вызывающий код увидел фатал
                        raise
                    log.warning(
                        "Ошибка вставки в %s (chunk=%d rows): %s — переподключаюсь и повторяю (%d/%d).",
                        table_fqn, len(chunk), e, attempt, self.insert_max_retries
                    )
                    self._connect_any()
        return total

# Совместимость
CHClient = ClickHouseClient
