import asyncio
import json
import os
import argparse
import logging
import gzip
import ssl
from math import ceil
from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement
from tqdm import tqdm


logger = logging.getLogger("cassandra_backup")


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
    def __init__(self, contact_points, username=None, password=None, port=9042,
                 ssl_context=None, idempotent=False, timeout=None,
                 read_consistency="LOCAL_ONE", write_consistency="QUORUM"):
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
        loop = asyncio.get_event_loop()
        self.session = await loop.run_in_executor(None, self.cluster.connect)
        logger.info("Подключение к кластеру Cassandra установлено")

    async def close(self):
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Соединение с кластером Cassandra закрыто")

    def _get_pk(self, keyspace, table_name):
        table_meta = self.cluster.metadata.keyspaces[keyspace].tables[table_name]
        return [col.name for col in table_meta.primary_key]

    def _exec(self, statement, write=False):
        """Выполнение запроса с нужным consistency level"""
        if isinstance(statement, (SimpleStatement, BatchStatement)):
            if write:
                statement.consistency_level = self.write_consistency
            else:
                statement.consistency_level = self.read_consistency
        return self.session.execute(statement, timeout=self.timeout)

    async def _backup_shard(self, keyspace, table_name, output_dir, fetch_size,
                            gzip_enabled, chunk_size, shard_id, start_token, end_token,
                            sem: asyncio.Semaphore, pbar: tqdm, shard_row_limit: int | None):
        async with sem:
            pk = ",".join(self._get_pk(keyspace, table_name))
            limit_clause = f" LIMIT {shard_row_limit}" if shard_row_limit else ""
            query = (
                f"SELECT * FROM {keyspace}.{table_name} "
                f"WHERE token({pk}) >= {start_token} AND token({pk}) < {end_token}"
                f"{limit_clause}"
            )
            stmt = SimpleStatement(query, fetch_size=fetch_size)
            stmt.is_idempotent = self.idempotent
            stmt.consistency_level = self.read_consistency
            rows = self._exec(stmt)

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

    async def _restore_table(self, keyspace: str, table_file: str, batch_size: int,
                             sem: asyncio.Semaphore, pbar: tqdm,
                             retries: int = 5, delay: int = 2):
        async with sem:
            table_name = os.path.basename(table_file).split("_part")[0]
            open_func = gzip.open if table_file.endswith(".gz") else open

            with open_func(table_file, "rt", encoding="utf-8") as f:
                data = json.load(f)

            if not data:
                logger.info(f"Таблица '{table_name}' пустая, пропускаем")
                return

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


def build_ssl_context(enable_ssl: bool, ca_cert: str | None, client_cert: str | None,
                      client_key: str | None, no_verify: bool) -> ssl.SSLContext | None:
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

    backup_parser = subparsers.add_parser("backup", help="Backup a keyspace")
    backup_parser.add_argument("keyspace")
    backup_parser.add_argument("output_dir")

    restore_parser = subparsers.add_parser("restore", help="Restore a keyspace")
    restore_parser.add_argument("keyspace")
    restore_parser.add_argument("input_dir")

    parser.add_argument("--hosts", required=True)
    parser.add_argument("--port", type=int, default=9042)
    parser.add_argument("--username")_
