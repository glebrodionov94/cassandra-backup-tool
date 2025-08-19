import asyncio
import json
import os
import argparse
import logging
import gzip
import ssl
from math import ceil
from typing import Optional

from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement
from tqdm import tqdm


logger = logging.getLogger("cassandra_backup")


# Карта строковых значений в уровни консистентности Cassandra
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


class CassandraBackup:
    def __init__(self, contact_points, username: Optional[str] = None, password: Optional[str] = None,
                 port: int = 9042, ssl_context: Optional[ssl.SSLContext] = None,
                 idempotent: bool = False, timeout: Optional[float] = None,
                 read_consistency: str = "LOCAL_ONE", write_consistency: str = "QUORUM"):
        """
        Инициализация подключения к Cassandra (TLS/SSL, идемпотентность, таймаут, консистентность).
        """
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

    async def connect(self):
        """Асинхронное подключение (через executor, т.к. драйвер sync)."""
        loop = asyncio.get_event_loop()
        self.session = await loop.run_in_executor(None, self.cluster.connect)
        logger.info("Подключение к кластеру Cassandra установлено")

    async def close(self):
        """Корректное закрытие кластера."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Соединение с кластером Cassandra закрыто")

    def _get_pk(self, keyspace: str, table_name: str):
        """Возвращает список колонок первичного ключа (partition+clustering) для token(...)."""
        table_meta = self.cluster.metadata.keyspaces[keyspace].tables[table_name]
        return [col.name for col in table_meta.primary_key]

    def _exec(self, statement, write: bool = False):
        """Выполнение запроса с нужным consistency level и общим timeout."""
        if isinstance(statement, (SimpleStatement, BatchStatement)):
            statement.consistency_level = self.write_consistency if write else self.read_consistency
        return self.session.execute(statement, timeout=self.timeout)

    async def _backup_shard(self, keyspace: str, table_name: str, output_dir: str, fetch_size: int,
                            gzip_enabled: bool, chunk_size: Optional[int], shard_id: int,
                            start_token: int, end_token: int, sem: asyncio.Semaphore,
                            pbar: tqdm, shard_row_limit: Optional[int]):
        """Выгрузка одного шарда таблицы по token диапазону с опциональным лимитом строк на шард."""
        async with sem:
            pk = ",".join(self._get_pk(keyspace, table_name))
            limit_clause = f" LIMIT {shard_row_limit}" if shard_row_limit else ""
            query = (
                f"SELECT * FROM {keyspace}.{table_name} "
                f"WHERE token({pk}) >= {start_token} AND token({pk}) < {end_token}"
                f"{limit_clause}"
            )
            stmt = SimpleStatement(query, fetch_size=fetch_size)
            stmt.is_idempotent = self.idempotent  # SELECT безопасно помечать идемпотентным
            stmt.consistency_level = self.read_consistency
            rows = self._exec(stmt, write=False)

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

            for row in rows:
                if shard_row_limit and local_count >= shard_row_limit:
                    break

                row_dict = dict(row._asdict())
                # Нормализуем коллекции к JSON-совместимым типам
                for k, v in row_dict.items():
                    if hasattr(v, "__iter__") and not isinstance(v, (str, bytes, dict)):
                        row_dict[k] = list(v)

                if not first:
                    f.write(",\n")
                f.write(json.dumps(row_dict, ensure_ascii=False))
                first = False

                local_count += 1
                chunk_count += 1
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
        """Выгрузка одной таблицы. Если shards > 1, делим на токен-диапазоны и читаем параллельно."""
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
                              limit_per_table: Optional[int] = None):
        """Бэкап keyspace (таблицы выборочно/все), шардирование и лимит строк."""
        os.makedirs(output_dir, exist_ok=True)
        keyspace_meta = self.cluster.metadata.keyspaces[keyspace]

        # Список таблиц
        if tables:
            selected = set(tables)
            table_names = [t for t in keyspace_meta.tables.keys() if t in selected]
            logger.info(f"Бэкап только таблиц: {', '.join(table_names)}")
        else:
            table_names = list(keyspace_meta.tables.keys())

        # Автовыбор шардов: 2 × число узлов, если не задано явно
        if shards is None:
            num_nodes = len(self.cluster.metadata.all_hosts)
            shards = max(1, num_nodes * 2)
            logger.info(f"Автоматически выбрано количество шардов: {shards} (узлов: {num_nodes})")
        else:
            logger.info(f"Используем заданное количество шардов: {shards}")

        # Оценка общего числа строк для прогресса
        total_rows = 0
        for table_name in table_names:
            try:
                stmt = SimpleStatement(f"SELECT count(*) FROM {keyspace}.{table_name}")
                stmt.is_idempotent = self.idempotent
                stmt.consistency_level = self.read_consistency
                row = self._exec(stmt, write=False).one()
                cnt = row[0] if row else 0
                if limit_per_table is not None:
                    cnt = min(cnt, limit_per_table)
                total_rows += cnt
            except Exception as e:
                logger.warning(f"Не удалось посчитать строки в {table_name}: {e}")

        # Сохраняем схему (keyspace, UDT, tables, indexes, materialized views)
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

        # Параллельная выгрузка таблиц
        sem = asyncio.Semaphore(parallel)
        with tqdm(total=total_rows, desc="Общий прогресс бэкапа", unit="строк") as pbar:
            tasks = [
                self._backup_table(keyspace, tname, output_dir,
                                   fetch_size, gzip_enabled, chunk_size,
                                   shards, sem, pbar,
                                   limit_per_table)
                for tname in table_names
            ]
            await asyncio.gather(*tasks)

        logger.info(f"✅ Бэкап keyspace '{keyspace}' завершён (оценочно строк: {total_rows})")

    async def _restore_table(self, keyspace: str, table_file: str, batch_size: Optional[int],
                             sem: asyncio.Semaphore, pbar: tqdm,
                             retries: int = 5, delay: int = 2):
        """Восстановление одной таблицы из файла (json/json.gz) с batch-вставками и retry."""
        async with sem:
            table_name = os.path.basename(table_file).split("_part")[0]
            open_func = gzip.open if table_file.endswith(".gz") else open

            with open_func(table_file, "rt", encoding="utf-8") as f:
                data = json.load(f)

            if not data:
                logger.info(f"Таблица '{table_name}' пустая, пропускаем")
                return

            # Автовыбор batch-size, если не задан
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

            columns = list(data[0].keys())
            col_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            prepared = self.session.prepare(
                f"INSERT INTO {keyspace}.{table_name} ({col_str}) VALUES ({placeholders})"
            )

            for i in range(0, len(data), bsize):
                batch = BatchStatement()
                batch.is_idempotent = self.idempotent
                batch.consistency_level = self.write_consistency
                for row in data[i:i + bsize]:
                    batch.add(prepared, tuple(row.values()))

                attempt = 0
                while attempt < retries:
                    try:
                        # Пишем с write=True для корректного consistency/timeout
                        self._exec(batch, write=True)
                        break
                    except Exception as e:
                        attempt += 1
                        wait = delay * (2 ** (attempt - 1))
                        logger.warning(
                            f"Ошибка batch вставки в {table_name} (попытка {attempt}/{retries}): {e} "
                            f"— жду {wait}с перед повтором"
                        )
                        await asyncio.sleep(wait)
                else:
                    logger.error(f"❌ Достигнут лимит retry при вставке в {table_name}, блок {i//bsize}")

                pbar.update(len(data[i:i + bsize]))

            logger.info(f"Таблица '{table_name}' восстановлена из {table_file} ({len(data)} строк)")

    async def restore_keyspace(self, keyspace: str, input_dir: str,
                               drop: bool = False, batch_size: Optional[int] = None,
                               parallel: int = 4, tables=None,
                               retries: int = 5, retry_delay: int = 2):
        """
        Восстановление keyspace: схема + данные из json/json.gz (части учитываются и сортируются).
        """
        if drop:
            logger.warning(f"Удаляю keyspace '{keyspace}' перед восстановлением")
            stmt = SimpleStatement(f"DROP KEYSPACE IF EXISTS {keyspace}")
            stmt.is_idempotent = self.idempotent
            stmt.consistency_level = self.write_consistency
            self._exec(stmt, write=True)

        # Восстанавливаем схему
        schema_file = os.path.join(input_dir, f"{keyspace}_schema.cql")
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_cql = f.read()

        for raw_stmt in schema_cql.split(";"):
            stmt_str = raw_stmt.strip()
            if stmt_str:
                try:
                    stmt = SimpleStatement(stmt_str)
                    stmt.is_idempotent = self.idempotent
                    stmt.consistency_level = self.write_consistency
                    self._exec(stmt, write=True)
                except Exception as e:
                    logger.error(f"Ошибка при выполнении CQL: {stmt_str[:80]}... | {e}")

        logger.info(f"Схема keyspace '{keyspace}' восстановлена")

        # Ищем все json/json.gz части
        all_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir)
                     if f.endswith(".json") or f.endswith(".json.gz")]

        # Фильтр по таблицам (если задано)
        if tables:
            selected = set(tables)
            files = [f for f in all_files if os.path.basename(f).split("_part")[0] in selected]
            logger.info(f"Восстанавливаем только таблицы: {', '.join(selected)}")
        else:
            files = all_files

        # Сортировка part1, part2, ...
        def sort_key(fname: str):
            base = os.path.basename(fname)
            if "_part" in base:
                prefix, part = base.split("_part")
                num = part.split(".")[0]
                try:
                    part_num = int(num)
                except ValueError:
                    part_num = 0
                return (prefix, part_num)
            return (base, 0)

        files.sort(key=sort_key)

        # Считаем общий объём строк (для прогресса)
        total_rows = 0
        for fpath in files:
            try:
                open_func = gzip.open if fpath.endswith(".gz") else open
                with open_func(fpath, "rt", encoding="utf-8") as fh:
                    total_rows += len(json.load(fh))
            except Exception:
                pass

        # Параллельная загрузка таблиц
        sem = asyncio.Semaphore(parallel)
        with tqdm(total=total_rows, desc="Общий прогресс восстановления", unit="строк") as pbar:
            tasks = [
                self._restore_table(keyspace, fpath, batch_size, sem, pbar,
                                    retries=retries, delay=retry_delay)
                for fpath in files
            ]
            await asyncio.gather(*tasks)

        logger.info(f"✅ Восстановление keyspace '{keyspace}' завершено (строк: {total_rows})")


def build_ssl_context(enable_ssl: bool, ca_cert: Optional[str], client_cert: Optional[str],
                      client_key: Optional[str], no_verify: bool) -> Optional[ssl.SSLContext]:
    """
    Создаёт SSLContext для кластера Cassandra.
    """
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


async def main():
    parser = argparse.ArgumentParser(description="Cassandra backup/restore tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # -------- backup CLI --------
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

    # -------- restore CLI --------
    restore_parser = subparsers.add_parser("restore", help="Restore a keyspace")
    restore_parser.add_argument("keyspace", help="Имя keyspace для восстановления")
    restore_parser.add_argument("input_dir", help="Каталог с бэкапом")
    restore_parser.add_argument("--drop", action="store_true", help="Удалить keyspace перед восстановлением")
    restore_parser.add_argument("--batch-size", type=int, help="Batch size для вставки (по умолчанию авто)")
    restore_parser.add_argument("--parallel", type=int, default=4, help="Макс. параллельных таблиц при restore (default=4)")
    restore_parser.add_argument("--tables", help="Список таблиц через запятую (по умолчанию все)")
    restore_parser.add_argument("--retries", type=int, default=5, help="Максимум попыток при ошибках вставки (default=5)")
    restore_parser.add_argument("--retry-delay", type=int, default=2, help="Начальная задержка перед повтором, сек (default=2)")

    # -------- connection / TLS / logging / control --------
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
    parser.add_argument("--timeout", type=float, help="Таймаут запросов (сек.) для execute()")

    # раздельная консистентность
    parser.add_argument("--read-consistency", default="LOCAL_ONE", help="Уровень консистентности для SELECT (default=LOCAL_ONE)")
    parser.add_argument("--write-consistency", default="QUORUM", help="Уровень консистентности для INSERT/DDL (default=QUORUM)")

    # -------- log level --------
    parser.add_argument("--log-level", default="INFO", help="Уровень логирования (DEBUG, INFO, WARNING, ERROR)")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    hosts = [h.strip() for h in args.hosts.split(",")]

    # Готовим SSLContext
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
        tables = args.tables.split(",") if args.tables else None
        await backup.backup_keyspace(args.keyspace, args.output_dir,
                                     tables=tables,
                                     fetch_size=args.fetch_size,
                                     gzip_enabled=args.gzip,
                                     chunk_size=args.chunk_size,
                                     parallel=args.parallel,
                                     shards=args.shards,
                                     limit_per_table=args.limit)
    elif args.command == "restore":
        tables = args.tables.split(",") if args.tables else None
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
