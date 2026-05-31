# Отчет по лабораторной работе N3

## Что реализовано

Реализована потоковая обработка данных с помощью Apache Flink:

1. Приложение `CsvToKafkaProducer` читает все CSV-файлы из папки `исходные данные`, преобразует каждую строку в JSON и отправляет сообщения в Kafka-топик `sales-json`.
2. Flink-джоба `FlinkStarSchemaJob` читает JSON-сообщения из Kafka, преобразует данные в модель "звезда" и записывает результат в PostgreSQL.
3. PostgreSQL инициализируется SQL-скриптом `sql/init.sql`.
4. Docker Compose поднимает PostgreSQL, Kafka, Flink JobManager, Flink TaskManager и сервис producer.

## Модель данных

В PostgreSQL создаются таблицы:

- `dim_customer` - измерение покупателей;
- `dim_seller` - измерение продавцов;
- `dim_product` - измерение товаров;
- `dim_store` - измерение магазинов;
- `dim_supplier` - измерение поставщиков;
- `fact_sales` - факт продаж со ссылками на измерения.

Так как в разных CSV-файлах исходные числовые `id` повторяются, в таблицах используются суррогатные ключи `*_key`, рассчитанные как стабильный UUID по полям сущности. Исходные числовые идентификаторы сохраняются в полях `source_*_id`.

## Запуск

Поднять PostgreSQL, Kafka и Flink:

```bash
docker compose up -d postgres zookeeper kafka jobmanager taskmanager
```

Отправить CSV-данные в Kafka:

```bash
docker compose run --rm producer
```

Собрать JAR с Flink-джобой:

```bash
docker run --rm -v ${PWD}:/app -w /app maven:3.9.9-eclipse-temurin-11 mvn -q "-Dmaven.test.skip=true" package
```

Скопировать JAR в контейнер JobManager:

```bash
docker compose exec jobmanager mkdir -p /opt/flink/usrlib
docker compose cp target/bigdata-flink-lab-1.0-SNAPSHOT.jar jobmanager:/opt/flink/usrlib/bigdata-flink-lab.jar
```

Запустить Flink-джобу:

```bash
docker compose exec jobmanager flink run -d -c ru.bigdataflink.FlinkStarSchemaJob /opt/flink/usrlib/bigdata-flink-lab.jar
```

Веб-интерфейс Flink доступен по адресу:

```text
http://localhost:8081
```

## Проверка данных

Проверить количество загруженных фактов:

```bash
docker compose exec postgres psql -U postgres -d salesdb -c "SELECT COUNT(*) FROM fact_sales;"
```

Посмотреть несколько строк фактов:

```bash
docker compose exec postgres psql -U postgres -d salesdb -c "SELECT * FROM fact_sales LIMIT 5;"
```

Проверить справочники:

```bash
docker compose exec postgres psql -U postgres -d salesdb -c "SELECT COUNT(*) FROM dim_customer;"
docker compose exec postgres psql -U postgres -d salesdb -c "SELECT COUNT(*) FROM dim_product;"
docker compose exec postgres psql -U postgres -d salesdb -c "SELECT COUNT(*) FROM dim_store;"
docker compose exec postgres psql -U postgres -d salesdb -c "SELECT COUNT(*) FROM dim_supplier;"
```

## Результат проверки

После запуска producer было отправлено `10000` сообщений в Kafka. После запуска Flink-джобы в PostgreSQL получено:

- `fact_sales` - `10000` строк;
- `dim_customer` - `10000` строк;
- `dim_seller` - `10000` строк;
- `dim_product` - `10000` строк;
- `dim_store` - `10000` строк;
- `dim_supplier` - `10000` строк.
