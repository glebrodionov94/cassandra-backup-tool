#!/usr/bin/env python3
import asyncio
import json
import os
import argparse
import logging
import gzip
import ssl
import base64
import re
from math import ceil
from typing import Optional, Iterable, List, Any
from decimal import Decimal
from uuid import UUID
from datetime import datetime

from cassandra.cluster import Cluster, ConsistencyLevel, ResultSet
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement
from cassandra.metadata import ColumnMetadata
from tqdm import tqdm

logger = logging.getLogger("cassandra_backup")

# -------------------- Константы/карты --------------------

CONSISTENCY_MAP = {
    "ANY": ConsistencyLevel.ANY,
    "ONE": ConsistencyLevel.ONE,
    "TWO": ConsistencyLevel.TWO,
    "THREE": ConsistencyLevel.THREE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
    "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
    "EACH_QUORUM": ConsistencyLevel.EACH_QUORUM,
    "SERIAL": ConsistencyLevel.SERIAL,
    "LOCAL_SERIAL": ConsistencyLevel.LOCAL_SERIAL,
    "LOCAL_ONE": ConsistencyLevel.LOCAL_ONE,
}

# pattern для имён файлов частей
_NAME_RE = re.compile(
    r'^(?P<table>.+?)(?:_shard(?P<shard>\d+))?_part(?P<part>\d+)\.json(?:\.gz)?$'
)

# -------------------- JSON (де)сериализация специальных типов --------------------

def _json_default(o: Any):
    if isinstance(o, datetime):
        return o.isoformat(timespec="microseconds")
    if isinstance(o, Decimal):
        return str(o)
    if isinstance(o, UUID):
        return str(o)
    if isinstance(o, (set, frozenset)):
        return list(o)
    if isinstance(o, (bytes, bytearray, memoryview)):
        return {"__blob__": True, "b64": base64.b64encode(bytes(o)).decode("ascii")}
    return o  # пусть попробует сериализовать как есть

def _coerce_value(col_meta: ColumnMetadata, v: Any):
    """
    Минимальный коэрсер типов JSON->Cassandra на основе метаданных колонки.
    Достаточно для uuid/decimal/timestamp/blob/set; остальное — как есть.
    """
    if v is None:
        return None

    # blob-обёртка от нашего энкодера
    if isinstance(v, dict) and v.get("__blob__") and "b64" in v:
        return base64.b64decode(v["b64"])

    t = col_meta.cql_type
    name = getattr(t, 'typename', str(t)).lower()

    try:
        if name in ('uuid', 'timeuuid') and isinstance(v, str):
            return UUID(v)
        if name == 'decimal' and isinstance(v, str):
            return Decimal(v)
        if name in ('timestamp', 'date') and isinstance(v, str):
            return datetime.fromisoformat(v)
        if name.startswith('set<') and isinstance(v, list):
            return set(v)
        # list/map — JSON‑совместимы
    except Exception as e:
        logger.warning(f"Не удалось привести значение для колонки {col_meta.name} типа {name}: {e}")
    return v

def _make_idempotent(stmt_str: str) -> str:
    """
    Добавляет IF NOT EXISTS для CREATE объектов, где это безопасно и уместно.
    """
    s = stmt_str.strip()
    u = s.upper()
    if u.startswith("CREATE TABLE ") and " IF NOT EXISTS " not in u:
        return s.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
    if u.startswith("CREATE INDEX ") and " IF NOT EXISTS " not in u:
        return s.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS", 1)
    if u.startswith("CREATE MATERIALIZED VIEW ") and " IF NOT EXISTS " not in u:
        return s.replace("CREATE MATERIALIZED VIEW", "CREATE MATERIALIZED VIEW IF NOT EXISTS", 1)
    # KEYSPACE обычно уже содержит IF NOT EXISTS в экспортерах драйвера
    return s

# -------------------- Класс бэкапа/восстановления --------------------

class CassandraBackup:
    def __init__(self, contact_points, username: Optional[str] = None, password: Optional[str] = None,
                 port: int = 9042, ssl_context: Optional[ssl.SSLContext] = None,
                 idempotent: bool = False, timeout: Optional[float] = None,
                 read_consistency: str = "LOCAL_ONE", write_consistency: str = "QUORUM"):
        auth_provider = None
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)

        self.cluster = Cluster(
            contact_points=contact_points,
            port=port,
            auth_provider=auth_provider,
            ssl_context=ssl_context
        )
        self.session = None
        self.idempotent = bool(idempotent)
        self.timeout = timeout

        self.read_consistency = CONSISTENCY_MAP.get(read_consistency.upper(), ConsistencyLevel.LOCAL_ONE)
        self.write_consistency = CONSISTENCY_MAP.get(write_consistency.upper(), ConsistencyLevel.QUORUM)

        self._pbar_lock = None  # будет установлен перед задачами

    # ---------- подключение/закрытие ----------

    async def connect(self):
        loop = asyncio.get_running_loop()
        self.session = await loop.run_in_executor(None, self.cluster.connect)
        logger.info("Подключение к кластеру Cassandra установлено")

    async def close(self):
        def _shutdown():
            try:
                if self.session:
                    self.session.shutdown()
            finally:
                self.cluster.shutdown()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _shutdown)
        logger.info("Соединение с кластером Cassandra закрыто")

    # ---------- async-обёртки над execute_async ----------

    async def _await_rf(self, response_future):
        """
        Надёжно ждём завершение ResponseFuture без колбэков на event loop.
        """
        def _wait():
            if self.timeout is not None:
                return response_future.result(timeout=self.timeout)
            return response_future.result()
        return await asyncio.to_thread(_wait)

    def _exec_async(self, statement, write: bool = False):
        if isinstance(statement, (SimpleStatement, BatchStatement)):
            statement.consistency_level = self.write_consistency if write else self.read_consistency
            statement.is_idempotent = self.idempotent
        return self.session.execute_async(statement, timeout=self.timeout)

    async def _iter_rows(self, stmt: SimpleStatement) -> Iterable:
        """
        Асинхронный итератор по всем строкам с поддержкой пагинации.
        В разных версиях драйвера первая страница может прийти list или ResultSet.
        """
        rf = self._exec_async(stmt, write=False)
        result = await self._await_rf(rf)

        def _page_rows_and_state(res):
            if hasattr(res, "current_rows"):
                return res.current_rows, res
            return res, None  # list без пагинации

        rows, state = _page_rows_and_state(result)
        for row in rows:
            yield row

        while state is not None and getattr(state, "has_more_pages", False):
            next_rf = state.fetch_next_page()
            next_res = await self._await_rf(next_rf)
            rows, state = _page_rows_and_state(next_res)
            for row in rows:
                yield row

    # ---------- schema agreement (кросс‑версионно) ----------

    def _wait_schema_agreement(self, timeout=240, poll=2) -> bool:
        """
        1) Пытается session.wait_for_schema_agreement(timeout).
        2) Фоллбэк — ручная проверка системных таблиц.
        """
        import time

        wait_fn = getattr(self.session, "wait_for_schema_agreement", None)
        if callable(wait_fn):
            try:
                return bool(wait_fn(timeout=timeout))
            except Exception as e:
                logger.debug(f"wait_for_schema_agreement failed, fallback to manual: {e}")

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                local = self.session.execute("SELECT schema_version FROM system.local")
                peers = self.session.execute("SELECT schema_version FROM system.peers")
                versions = {row.schema_version for row in local} | {row.schema_version for row in peers}
                versions.discard(None)
                if len(versions) <= 1:
                    return True
            except Exception as e:
                logger.debug(f"manual schema agreement check failed: {e}")
            time.sleep(poll)

        logger.warning("Schema agreement not reached within timeout")
        return False

    # ---------- Метаданные ----------

    def _get_partition_key(self, keyspace: str, table_name: str) -> List[str]:
        table_meta = self.cluster.metadata.keyspaces[keyspace].tables[table_name]
        return [col.name for col in table_meta.partition_key]

    # ---------- Бэкап ----------

    async def _backup_shard(self, keyspace: str, table_name: str, output_dir: str, fetch_size: int,
                            gzip_enabled: bool, chunk_size: Optional[int], shard_id: int,
                            start_token: int, end_token: int, sem: asyncio.Semaphore,
                            pbar: tqdm, shard_row_limit: Optional[int]):
        async with sem:
            pk_cols = self._get_partition_key(keyspace, table_name)
            if not pk_cols:
                logger.warning(f"Таблица {table_name}: отсутствует partition key? Пропускаю.")
                return
            pk = ",".join(pk_cols)

            limit_clause = f" LIMIT {shard_row_limit}" if shard_row_limit else ""
            query = (
                f"SELECT * FROM {keyspace}.{table_name} "
                f"WHERE token({pk}) >= {start_token} AND token({pk}) < {end_token}"
                f"{limit_clause}"
            )
            stmt = SimpleStatement(query, fetch_size=fetch_size)
            stmt.is_idempotent = self.idempotent
            stmt.consistency_level = self.read_consistency

            part = 1
            local_count = 0
            chunk_count = 0
            ext = ".json.gz" if gzip_enabled else ".json"
            open_func = gzip.open if gzip_enabled else open

            def new_file():
                filename = os.path.join(output_dir, f"{table_name}_shard{shard_id}_part{part}{ext}")
                f = open_func(filename, "wt", encoding="utf-8")
                f.write("[\n")
                return f, filename

            f, filename = new_file()
            first = True

            async for row in self._iter_rows(stmt):
                if shard_row_limit and local_count >= shard_row_limit:
                    break

                row_dict = dict(row._asdict())
                # Мягкая нормализация коллекций
                for k, v in list(row_dict.items()):
                    if isinstance(v, (set, frozenset)):
                        row_dict[k] = list(v)

                if not first:
                    f.write(",\n")
                f.write(json.dumps(row_dict, ensure_ascii=False, default=_json_default))
                first = False

                local_count += 1
                chunk_count += 1

                if self._pbar_lock:
                    async with self._pbar_lock:
                        pbar.update(1)
                else:
                    pbar.update(1)

                if chunk_size and chunk_count >= chunk_size:
                    f.write("\n]\n")
                    f.close()
                    logger.info(
                        f"Таблица '{table_name}' шард {shard_id} часть {part} сохранена ({chunk_count} строк)"
                    )
                    part += 1
                    chunk_count = 0
                    f, filename = new_file()
                    first = True

            f.write("\n]\n")
            f.close()
            logger.info(
                f"Таблица '{table_name}' шард {shard_id} сохранена ({local_count} строк, файлов: {part})"
            )

    async def _backup_table(self, keyspace: str, table_name: str, output_dir: str,
                            fetch_size: int, gzip_enabled: bool, chunk_size: Optional[int],
                            shards: int, sem: asyncio.Semaphore, pbar: tqdm,
                            table_limit: Optional[int]):
        shard_row_limit = ceil(table_limit / max(1, shards)) if table_limit else None

        if shards and shards > 1:
            min_token = -2**63
            max_token = 2**63 - 1
            step = (max_token - min_token) // shards

            tasks = []
            for shard_id in range(shards):
                start_token = min_token + shard_id * step
                end_token = start_token + step if shard_id < shards - 1 else max_token
                tasks.append(
                    self._backup_shard(keyspace, table_name, output_dir, fetch_size,
                                       gzip_enabled, chunk_size, shard_id,
                                       start_token, end_token, sem, pbar,
                                       shard_row_limit)
                )
            await asyncio.gather(*tasks)
        else:
            await self._backup_shard(keyspace, table_name, output_dir, fetch_size,
                                     gzip_enabled, chunk_size, 0,
                                     -2**63, 2**63 - 1, sem, pbar, table_limit)

    async def backup_keyspace(self, keyspace: str, output_dir: str,
                              tables=None, fetch_size: int = 5000,
                              gzip_enabled: bool = False, chunk_size: Optional[int] = None,
                              parallel: int = 2, shards: Optional[int] = None,
                              limit_per_table: Optional[int] = None,
                              estimate_progress: bool = False):
        os.makedirs(output_dir, exist_ok=True)
        keyspace_meta = self.cluster.metadata.keyspaces[keyspace]

        # Список таблиц
        if tables:
            selected = set(tables)
            table_names = [t for t in keyspace_meta.tables.keys() if t in selected]
            missing = selected - set(table_names)
            if missing:
                logger.warning(f"Некоторые таблицы не найдены и будут пропущены: {', '.join(sorted(missing))}")
            logger.info(f"Бэкап только таблиц: {', '.join(table_names)}")
        else:
            table_names = list(keyspace_meta.tables.keys())

        # Авто-шарды: 2 × число узлов (кросс‑версионно для all_hosts)
        if shards is None:
            meta = self.cluster.metadata
            hosts_obj = meta.all_hosts() if callable(getattr(meta, "all_hosts", None)) else meta.all_hosts
            num_nodes = len(list(hosts_obj))
            shards = max(1, num_nodes * 2)
            logger.info(f"Автоматически выбрано количество шардов: {shards} (узлов: {num_nodes})")
        else:
            logger.info(f"Используем заданное количество шардов: {shards}")

        # Схема KS/UDT/таблицы/индексы/MV
        schema_file = os.path.join(output_dir, f"{keyspace}_schema.cql")
        with open(schema_file, "w", encoding="utf-8") as f:
            f.write(f"{keyspace_meta.export_as_string()};\n\n")
            for udt in keyspace_meta.user_types.values():
                f.write(f"{udt.as_cql_query()};\n\n")
            for tname in table_names:
                f.write(f"{keyspace_meta.tables[tname].as_cql_query()};\n\n")
                for index in keyspace_meta.tables[tname].indexes.values():
                    f.write(f"{index.as_cql_query()};\n\n")
            for mv in keyspace_meta.views.values():
                f.write(f"{mv.as_cql_query()};\n\n")
        logger.info(f"Схема keyspace '{keyspace}' сохранена в {schema_file}")

        # Прогресс без COUNT(*); опциональная грубая оценка
        total_rows = None
        if estimate_progress and limit_per_table:
            total_rows = limit_per_table * len(table_names)
            logger.info("Включена грубая оценка прогресса (total).")

        self._pbar_lock = asyncio.Lock()
        sem = asyncio.Semaphore(parallel)
        desc = "Общий прогресс бэкапа"
        with tqdm(total=total_rows, desc=desc, unit="строк") as pbar:
            tasks = [
                self._backup_table(keyspace, tname, output_dir,
                                   fetch_size, gzip_enabled, chunk_size,
                                   shards, sem, pbar,
                                   limit_per_table)
                for tname in table_names
            ]
            await asyncio.gather(*tasks)

        logger.info(f"✅ Бэкап keyspace '{keyspace}' завершён")

    # ---------- Восстановление ----------

    async def _restore_table(self, keyspace: str, table_file: str, batch_size: Optional[int],
                            sem: asyncio.Semaphore, pbar: tqdm,
                            retries: int = 5, delay: int = 2):
        async with sem:
            base = os.path.basename(table_file)
            open_func = gzip.open if table_file.endswith(".gz") else open

            # 1) Надёжно извлекаем имя таблицы из имени файла
            m = _NAME_RE.match(base)  # r'^(?P<table>.+?)(?:_shard(?P<shard>\d+))?_part(?P<part>\d+)\.json(?:\.gz)?$'
            if m:
                table_name = m.group('table')
            else:
                # Фоллбэк на всякий случай: отрезаем "_part…" и опциональное "_shard<id>"
                stem = base
                if "_part" in stem:
                    stem = stem[:stem.rfind("_part")]
                stem = re.sub(r'_shard\d+$', '', stem)
                stem = re.sub(r'\.json(\.gz)?$', '', stem)
                table_name = stem

            # 2) Читаем JSON
            with open_func(table_file, "rt", encoding="utf-8") as f:
                data = json.load(f)

            if not data:
                logger.info(f"Таблица '{table_name}' пустая, пропускаем")
                return

            # 3) Проверяем, что таблица действительно есть в схеме
            ks_meta = self.cluster.metadata.keyspaces.get(keyspace)
            if not ks_meta or table_name not in ks_meta.tables:
                logger.error(
                    f"Файл данных '{base}' ссылается на таблицу '{table_name}', которой нет в схеме keyspace '{keyspace}'. Пропускаю."
                )
                return

            # 4) Выбираем batch-size
            if batch_size is None:
                n = len(data)
                if n < 10_000:
                    bsize = 100
                elif n < 100_000:
                    bsize = 200
                elif n < 1_000_000:
                    bsize = 500
                else:
                    bsize = 1000
                logger.info(f"Автоматически выбран batch-size={bsize} для таблицы {table_name} (строк: {n})")
            else:
                bsize = batch_size
                logger.info(f"Используем заданный batch-size={bsize} для таблицы {table_name}")

            # 5) Готовим prepared statement и коэрсинг типов по метаданным
            columns = list(data[0].keys())
            col_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            table_meta = ks_meta.tables[table_name]
            col_metas = [table_meta.columns[c] for c in columns]

            prepared = self.session.prepare(
                f"INSERT INTO {keyspace}.{table_name} ({col_str}) VALUES ({placeholders})"
            )

            # 6) Вставка батчами с retry
            for i in range(0, len(data), bsize):
                batch = BatchStatement()
                batch.is_idempotent = self.idempotent
                batch.consistency_level = self.write_consistency

                slice_rows = data[i:i + bsize]
                for row in slice_rows:
                    vals = tuple(_coerce_value(cm, row.get(c)) for c, cm in zip(columns, col_metas))
                    batch.add(prepared, vals)

                attempt = 0
                while attempt < retries:
                    try:
                        rf = self._exec_async(batch, write=True)
                        await self._await_rf(rf)
                        break
                    except Exception as e:
                        attempt += 1
                        wait = delay * (2 ** (attempt - 1))
                        logger.warning(
                            f"Ошибка batch вставки в {table_name} (попытка {attempt}/{retries}): {e} — жду {wait}с перед повтором"
                        )
                        await asyncio.sleep(wait)
                else:
                    logger.error(f"❌ Достигнут лимит retry при вставке в {table_name}, блок {i//bsize}")

                if self._pbar_lock:
                    async with self._pbar_lock:
                        pbar.update(len(slice_rows))
                else:
                    pbar.update(len(slice_rows))

            logger.info(f"Таблица '{table_name}' восстановлена из {table_file} ({len(data)} строк)")


    async def restore_keyspace(self, keyspace: str, input_dir: str,
                               drop: bool = False, batch_size: Optional[int] = None,
                               parallel: int = 4, tables=None,
                               retries: int = 5, retry_delay: int = 2):
        # DROP (опционально) — DDL + schema agreement
        if drop:
            logger.warning(f"Удаляю keyspace '{keyspace}' перед восстановлением")
            stmt = SimpleStatement(f"DROP KEYSPACE IF EXISTS {keyspace}")
            stmt.is_idempotent = self.idempotent
            stmt.consistency_level = self.write_consistency
            rf = self._exec_async(stmt, write=True)
            try:
                await self._await_rf(rf)
            finally:
                self._wait_schema_agreement()

        # Восстанавливаем схему
        schema_file = os.path.join(input_dir, f"{keyspace}_schema.cql")
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_cql = f.read()

        # Простая сегментация по ';'
        for raw_stmt in schema_cql.split(";"):
            stmt_str = raw_stmt.strip()
            if not stmt_str:
                continue
            # пропуски предупреждений/комментариев
            if stmt_str.upper().startswith("WARNING") or stmt_str.startswith("--"):
                continue
            try:
                stmt_str = _make_idempotent(stmt_str)
                stmt = SimpleStatement(stmt_str)
                stmt.is_idempotent = self.idempotent
                stmt.consistency_level = self.write_consistency
                rf = self._exec_async(stmt, write=True)
                await self._await_rf(rf)
                # после каждого DDL — дождаться agreement
                self._wait_schema_agreement()
            except Exception as e:
                logger.error(f"Ошибка при выполнении CQL: {stmt_str[:180]}... | {e}")

        logger.info(f"Схема keyspace '{keyspace}' восстановлена")

        # Собираем файлы данных
        all_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir)
                     if f.endswith(".json") or f.endswith(".json.gz")]

        # Фильтр таблиц (если задан)
        if tables:
            selected = set(tables)
            files = [f for f in all_files if os.path.basename(f).split("_part")[0] in selected]
            logger.info(f"Восстанавливаем только таблицы: {', '.join(sorted(selected))}")
        else:
            files = all_files

        # Сортировка по table, shard, part (надёжно, с regex)
        def sort_key(fname: str):
            base = os.path.basename(fname)
            m = _NAME_RE.match(base)
            if not m:
                return (base, 10**9, 10**9)
            table = m.group('table')
            shard = int(m.group('shard') or 0)
            part = int(m.group('part'))
            return (table, shard, part)

        files.sort(key=sort_key)

        # Параллельная загрузка
        self._pbar_lock = asyncio.Lock()
        sem = asyncio.Semaphore(parallel)
        with tqdm(total=None, desc="Общий прогресс восстановления", unit="строк") as pbar:
            tasks = [
                self._restore_table(keyspace, fpath, batch_size, sem, pbar,
                                    retries=retries, delay=retry_delay)
                for fpath in files
            ]
            await asyncio.gather(*tasks)

        logger.info(f"✅ Восстановление keyspace '{keyspace}' завершено")

# -------------------- TLS --------------------

def build_ssl_context(enable_ssl: bool, ca_cert: Optional[str], client_cert: Optional[str],
                      client_key: Optional[str], no_verify: bool) -> Optional[ssl.SSLContext]:
    if not enable_ssl:
        return None

    if no_verify:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    else:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED

    if client_cert and client_key:
        ctx.load_cert_chain(certfile=client_cert, keyfile=client_key)

    return ctx

# -------------------- CLI --------------------

async def main():
    parser = argparse.ArgumentParser(description="Cassandra backup/restore tool (async)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # backup
    backup_parser = subparsers.add_parser("backup", help="Backup a keyspace")
    backup_parser.add_argument("keyspace", help="Имя keyspace")
    backup_parser.add_argument("output_dir", help="Каталог для бэкапа")
    backup_parser.add_argument("--tables", help="Список таблиц через запятую (по умолчанию все)")
    backup_parser.add_argument("--fetch-size", type=int, default=5000, help="Размер страницы выборки (default=5000)")
    backup_parser.add_argument("--gzip", action="store_true", help="Сохранять данные в gzip (.json.gz)")
    backup_parser.add_argument("--chunk-size", type=int, help="Макс строк в одном файле (например 1000000)")
    backup_parser.add_argument("--parallel", type=int, default=2, help="Макс. параллельных таблиц при бэкапе (default=2)")
    backup_parser.add_argument("--shards", type=int, help="Параллельных шардов на таблицу (по умолчанию = 2 × число узлов)")
    backup_parser.add_argument("--limit", type=int, help="Макс. строк на таблицу (распределяется по шардам)")
    backup_parser.add_argument("--estimate-progress", action="store_true",
                               help="Грубая оценка total для прогресса (иначе без total)")

    # restore
    restore_parser = subparsers.add_parser("restore", help="Restore a keyspace")
    restore_parser.add_argument("keyspace", help="Имя keyspace для восстановления")
    restore_parser.add_argument("input_dir", help="Каталог с бэкапом")
    restore_parser.add_argument("--drop", action="store_true", help="Удалить keyspace перед восстановлением")
    restore_parser.add_argument("--batch-size", type=int, help="Batch size для вставки (по умолчанию авто)")
    restore_parser.add_argument("--parallel", type=int, default=4, help="Макс. параллельных таблиц при restore (default=4)")
    restore_parser.add_argument("--tables", help="Список таблиц через запятую (по умолчанию все)")
    restore_parser.add_argument("--retries", type=int, default=5, help="Максимум попыток при ошибках вставки (default=5)")
    restore_parser.add_argument("--retry-delay", type=int, default=2, help="Начальная задержка перед повтором, сек (default=2)")

    # connection / TLS / logging / control
    parser.add_argument("--hosts", required=True, help="Список хостов Cassandra через запятую")
    parser.add_argument("--port", type=int, default=9042, help="Порт Cassandra (default=9042)")
    parser.add_argument("--username", help="Логин (опционально)")
    parser.add_argument("--password", help="Пароль (опционально)")
    parser.add_argument("--ssl", action="store_true", help="Включить TLS/SSL для подключения")
    parser.add_argument("--ca-cert", help="Путь к CA сертификату (PEM)")
    parser.add_argument("--client-cert", help="Путь к клиентскому сертификату (PEM) для mTLS")
    parser.add_argument("--client-key", help="Путь к приватному ключу (PEM) для mTLS")
    parser.add_argument("--ssl-no-verify", action="store_true", help="Отключить проверку сертификата/имени хоста (НЕ рек.)")
    parser.add_argument("--idempotent", action="store_true", help="Пометить запросы как идемпотентные")
    parser.add_argument("--timeout", type=float, help="Таймаут запросов (сек.) для execute_async()")

    # раздельная консистентность
    parser.add_argument("--read-consistency", default="LOCAL_ONE", help="Уровень консистентности для SELECT (default=LOCAL_ONE)")
    parser.add_argument("--write-consistency", default="QUORUM", help="Уровень консистентности для INSERT/DDL (default=QUORUM)")

    # log level
    parser.add_argument("--log-level", default="INFO", help="Уровень логирования (DEBUG, INFO, WARNING, ERROR)")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    hosts = [h.strip() for h in args.hosts.split(",")]

    ssl_ctx = build_ssl_context(
        enable_ssl=args.ssl,
        ca_cert=args.ca_cert,
        client_cert=args.client_cert,
        client_key=args.client_key,
        no_verify=args.ssl_no_verify
    )
    if args.ssl:
        mode = "NO VERIFY" if args.ssl_no_verify else "VERIFY"
        logger.info(f"TLS/SSL включён (mode: {mode}; CA: {args.ca_cert or 'system default'})")

    backup = CassandraBackup(
        contact_points=hosts,
        username=args.username,
        password=args.password,
        port=args.port,
        ssl_context=ssl_ctx,
        idempotent=args.idempotent,
        timeout=args.timeout,
        read_consistency=args.read_consistency,
        write_consistency=args.write_consistency
    )
    await backup.connect()

    if args.command == "backup":
        tables = [t.strip() for t in args.tables.split(",")] if args.tables else None
        await backup.backup_keyspace(args.keyspace, args.output_dir,
                                     tables=tables,
                                     fetch_size=args.fetch_size,
                                     gzip_enabled=args.gzip,
                                     chunk_size=args.chunk_size,
                                     parallel=args.parallel,
                                     shards=args.shards,
                                     limit_per_table=args.limit,
                                     estimate_progress=args.estimate_progress)
    elif args.command == "restore":
        tables = [t.strip() for t in args.tables.split(",")] if args.tables else None
        await backup.restore_keyspace(args.keyspace, args.input_dir,
                                      drop=args.drop,
                                      batch_size=args.batch_size,
                                      parallel=args.parallel,
                                      tables=tables,
                                      retries=args.retries,
                                      retry_delay=args.retry_delay)

    await backup.close()

if __name__ == "__main__":
    asyncio.run(main())
