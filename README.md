# Cassandra Backup Tool

Асинхронный инструмент для **бэкапа** и **восстановления** keyspace Cassandra с поддержкой:

* сохранения схемы и данных, включая репликацию и UDT;
* **параллельной выгрузки** таблиц и **шардирования по token range**;
* сохранения данных в **JSON** или **GZIP** с разбиением на чанки;
* восстановления с **batch insert**, автоподбором размера батча и **retry с паузой**;
* выбора **consistency level** для чтения и записи;
* поддержки **TLS/SSL (mTLS)**;
* прогресса через `tqdm` и логирования.

---

## Установка

```bash
git clone https://github.com/glebrodionov94/cassandra-backup-tool.git
cd cassandra-backup
pip install cassandra-driver tqdm
```

---

## Использование

Скрипт предоставляет две команды: `backup` и `restore`.

### 🔹 Бэкап keyspace

```bash
python cassandra_backup.py backup KEYSPACE OUTPUT_DIR \
  --hosts node1,node2 \
  --username cassandra --password secret \
  --gzip \
  --chunk-size 1000000 \
  --parallel 4 \
  --limit 200000 \
  --read-consistency LOCAL_QUORUM \
  --idempotent \
  --timeout 20
```

Параметры:

* `KEYSPACE` – имя keyspace;
* `OUTPUT_DIR` – каталог для бэкапа;
* `--hosts` – список узлов Cassandra через запятую;
* `--username / --password` – учётные данные (опционально);
* `--gzip` – сохранять файлы в `.json.gz`;
* `--chunk-size` – максимальное число строк в одном файле;
* `--parallel` – количество одновременно выгружаемых таблиц;
* `--shards` – число параллельных шардов на таблицу (по умолчанию = `2 × количество узлов`);
* `--limit` – ограничение строк на таблицу;
* `--read-consistency` – уровень консистентности для SELECT;
* `--idempotent` – помечает SELECT как идемпотентные;
* `--timeout` – таймаут в секундах для выполнения запроса.

---

### 🔹 Восстановление keyspace

```bash
python cassandra_backup.py restore KEYSPACE INPUT_DIR \
  --hosts node1 \
  --username cassandra --password secret \
  --drop \
  --parallel 4 \
  --retries 7 \
  --retry-delay 3 \
  --write-consistency ALL \
  --idempotent \
  --timeout 15
```

Параметры:

* `KEYSPACE` – имя keyspace для восстановления;
* `INPUT_DIR` – каталог с бэкапом;
* `--drop` – удалить keyspace перед восстановлением;
* `--batch-size` – размер batch вставки (по умолчанию определяется автоматически);
* `--parallel` – число таблиц для одновременного восстановления;
* `--retries` – количество попыток при ошибках вставки;
* `--retry-delay` – начальная задержка (увеличивается экспоненциально);
* `--write-consistency` – уровень консистентности для INSERT/DDL;
* `--idempotent` – помечает INSERT как идемпотентные;
* `--timeout` – таймаут на выполнение запроса.

---

## TLS / SSL

Поддерживается шифрование соединения:

```bash
python cassandra_backup.py backup myks ./backups \
  --hosts node1,node2 \
  --ssl \
  --ca-cert /etc/certs/ca.pem \
  --client-cert /etc/certs/client.pem \
  --client-key /etc/certs/client.key
```

Флаги:

* `--ssl` – включить TLS;
* `--ca-cert` – CA для проверки сервера;
* `--client-cert` / `--client-key` – клиентский сертификат для mTLS;
* `--ssl-no-verify` – отключить проверку сертификата (не рекомендуется).

---

## Логирование

Уровень логов задаётся параметром `--log-level` (по умолчанию `INFO`).

Пример с отладкой:

```bash
python cassandra_backup.py backup myks ./backups --hosts node1 --log-level DEBUG
```

---

## Возможности

* [x] Полный бэкап keyspace (схема + данные)
* [x] Выборочные таблицы
* [x] Gzip и чанки по строкам
* [x] Параллельный бэкап таблиц
* [x] Автоматическое шардирование по числу узлов кластера
* [x] Автовыбор batch-size при restore
* [x] Retry с экспоненциальной паузой
* [x] TLS/mTLS поддержка
* [x] Раздельный consistency level для SELECT/INSERT

---

## TODO / Roadmap

* [ ] Поддержка S3/MinIO как хранилища бэкапов
* [ ] Инкрементальные бэкапы
* [ ] Совместимость с ScyllaDB