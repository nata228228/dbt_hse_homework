# -*- coding: utf-8 -*-
# Проверки из ноутбука Practice_7_spark.ipynb, вынесенные в отдельный скрипт.
# Запуск: spark-submit checks.py --check <name> --load-date YYYY-MM-DD --log-file <path>
# Результат дописывается в лог (JSONL) и печатается в stdout.

import argparse
import json
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = 'de-course'
P = lambda rel: f"s3a://{BUCKET}/{rel}"


def make_spark(delta=False):
    b = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
         .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
         .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))
    if delta:
        b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return b.getOrCreate()


def window(load_date):
    return load_date - timedelta(days=3), load_date


# ---------------- RAW ----------------

def check_raw_customers(spark, load_date, s, e):
    df = spark.read.parquet(P("raw/customers"))
    dups = df.groupBy("customer_id").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты customer_id: {dups} - снимок не идемпотентен"
    return f"OK: {df.count()} строк, дубликатов customer_id нет"


def check_raw_orders(spark, load_date, s, e):
    df = spark.read.parquet(P("raw/orders"))
    df_window = df.filter((F.col("order_created_dt") >= s) & (F.col("order_created_dt") < e))
    dups = df_window.groupBy("order_id", "order_created_dt").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты order_id в окне [{s}; {e}): {dups}"
    n_window = df_window.count()
    assert n_window > 0, f"пусто окно [{s}; {e})"
    n_outside = df.filter((F.col("order_created_dt") < s) | (F.col("order_created_dt") >= e)).count()
    return (f"OK: всего {df.count()} строк (вне окна от прошлых запусков: {n_outside}), "
            f"окно [{s}; {e}) = {n_window} строк, дубликатов нет")


def check_raw_payments(spark, load_date, s, e):
    df = spark.read.parquet(P("raw/payments"))
    df_window = df.filter(((F.col("paid_at") >= s) & (F.col("paid_at") < e)) | F.col("paid_at").isNull())
    dups = df_window.groupBy("payment_id").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты payment_id в окне [{s}; {e}) + pending: {dups}"
    n_pending = df.filter(F.col("paid_at").isNull()).count()
    assert n_pending > 0, "pending-платежи потеряны - проверь ветку OR paid_at IS NULL в where"
    n_window = df_window.count()
    assert n_window > 0, f"пусто окно [{s}; {e}) + pending"
    return (f"OK: всего {df.count()} строк, окно [{s}; {e}) + pending = {n_window} строк, "
            f"pending: {n_pending}, дубликатов нет")


def check_raw_events(spark, load_date, s, e):
    df = spark.read.parquet(P("raw/events"))
    # event_dt = date(event_time), event_time NOT NULL -> пустых партиций быть не должно
    default_parts = df.filter(F.col("event_dt").isNull()).count()
    assert default_parts == 0, "есть строки с пустым event_dt - проверь партиционирование"
    df_window = df.filter((F.col("event_dt") >= s) & (F.col("event_dt") < e))
    dups = df_window.groupBy("event_id").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты event_id в окне [{s}; {e}): {dups}"
    n_window = df_window.count()
    assert n_window > 0, f"пусто окно [{s}; {e})"
    n_outside = df.filter((F.col("event_dt") < s) | (F.col("event_dt") >= e)).count()
    return (f"OK: всего {df.count()} строк (вне окна от прошлых запусков: {n_outside}), "
            f"окно [{s}; {e}) = {n_window} строк, дубликатов event_id нет")


def check_raw_products(spark, load_date, s, e):
    df = spark.read.parquet(P("raw/products"))
    dups = df.groupBy("product_id").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты product_id: {dups} - снимок не идемпотентен"
    n = df.count()
    assert n > 0, "справочник products пуст"
    return f"OK: {n} строк, дубликатов product_id нет"


def check_raw_product_categories(spark, load_date, s, e):
    df = spark.read.parquet(P("raw/product_categories"))
    dups = df.groupBy("category_id").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты category_id: {dups} - снимок не идемпотентен"
    name_dups = df.groupBy("category_name").count().filter("count > 1").count()
    assert name_dups == 0, f"дубликаты category_name: {name_dups} (category_name UNIQUE в источнике)"
    # ссылочная целостность внутри справочника: parent_category_id либо NULL, либо существующий category_id
    known = set(r["category_id"] for r in df.select("category_id").collect())
    orphans = df.filter(F.col("parent_category_id").isNotNull()) \
                .select("parent_category_id").distinct() \
                .filter(~F.col("parent_category_id").isin(list(known))).count()
    assert orphans == 0, f"parent_category_id ссылается на несуществующую категорию: {orphans} строк"
    n = df.count()
    assert n > 0, "справочник product_categories пуст"
    return f"OK: {n} строк, дубликатов category_id/category_name нет, ссылки на родителя корректны"


def check_raw_order_items(spark, load_date, s, e):
    df = spark.read.parquet(P("raw/order_items"))
    dups = df.groupBy("order_id", "product_id").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты (order_id, product_id): {dups} - снимок не идемпотентен"
    n = df.count()
    assert n > 0, "raw/order_items пуст"
    bad_qty = df.filter((F.col("quantity").isNull()) | (F.col("quantity") <= 0)).count()
    assert bad_qty == 0, f"строк с quantity <= 0 или NULL: {bad_qty}"
    bad_price = df.filter(F.col("unit_price").isNull() | (F.col("unit_price") < 0)).count()
    assert bad_price == 0, f"строк с unit_price < 0 или NULL: {bad_price}"
    bad_disc = df.filter(F.col("discount_amount").isNull() | (F.col("discount_amount") < 0)).count()
    assert bad_disc == 0, f"строк с discount_amount < 0 или NULL: {bad_disc}"
    # аналогично CHECK (discount_amount <= quantity * unit_price) в источнике
    bad_sum = df.filter(F.col("discount_amount") > F.col("quantity") * F.col("unit_price")).count()
    assert bad_sum == 0, f"строк, где discount_amount > quantity * unit_price: {bad_sum}"
    return f"OK: {n} строк, дубликатов (order_id, product_id) нет, числовые ограничения соблюдены"

# ---------------- STAGING ----------------

def check_stg_orders(spark, load_date, s, e):
    path = P("staging/stg_orders")
    df = spark.read.parquet(path)
    assert len(df.columns) == 7, f"ожидалось 7 колонок, получено: {df.columns}"
    dtypes = dict(df.dtypes)
    assert dtypes["order_date"] == "date", f"order_date должен быть date, получен {dtypes['order_date']}"
    assert dtypes["is_test_order"] == "boolean", f"is_test_order должен быть boolean, получен {dtypes['is_test_order']}"
    df_window = df.filter((F.col("order_date") >= s) & (F.col("order_date") < e))
    bounds = df_window.agg(F.min("order_date").alias("mn"), F.max("order_date").alias("mx")).first()
    assert bounds["mn"] >= s and bounds["mx"] < e, \
        f"данные окна выходят за границы [{s}; {e}): [{bounds['mn']}; {bounds['mx']}]"
    n_part = spark.read.parquet(f"{path}/order_date={s}").count()
    assert n_part > 0, f"пуста партиция order_date={s}"
    n_src = (spark.read.parquet(P("raw/orders"))
             .filter((F.col("order_created_dt") >= s) & (F.col("order_created_dt") < e)).count())
    assert df_window.count() == n_src, \
        f"число строк за окно [{s}; {e}) разошлось: {n_src} -> {df_window.count()}"
    dups = df_window.groupBy("order_id", "order_date").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты (order_id, order_date) в окне: {dups}"
    return (f"OK: {df_window.count()} строк за окно [{s}; {e}) - совпадает с источником, "
            f"схема корректна, дубликатов нет")


def check_stg_payments(spark, load_date, s, e):
    path = P("staging/stg_payments")
    df = spark.read.parquet(path)
    assert len(df.columns) == 7, f"ожидалось 7 колонок, получено {len(df.columns)}"
    dtypes = dict(df.dtypes)
    assert dtypes["is_refund"] == "boolean", f"is_refund должен быть boolean, получен {dtypes['is_refund']}"
    assert dtypes["paid_dt"] == "date", f"paid_dt должен быть date, получен {dtypes['paid_dt']}"
    df_window = df.filter(((F.col("paid_dt") >= s) & (F.col("paid_dt") < e)) | F.col("paid_dt").isNull())
    bounds = df_window.filter(F.col("paid_dt").isNotNull()).agg(
        F.min("paid_dt").alias("mn"), F.max("paid_dt").alias("mx")).first()
    assert bounds["mn"] >= s and bounds["mx"] < e, \
        f"данные окна выходят за границы [{s}; {e}): [{bounds['mn']}; {bounds['mx']}]"
    n_part = spark.read.parquet(f"{path}/paid_dt={s}").count()
    assert n_part > 0, f"пуста партиция paid_dt={s}"
    n_src = (spark.read.parquet(P("raw/payments"))
             .filter(((F.col("paid_dt") >= s) & (F.col("paid_dt") < e)) | F.col("paid_dt").isNull()).count())
    assert df_window.count() == n_src, \
        f"число строк за окно [{s}; {e}) разошлось: {n_src} -> {df_window.count()}"
    dups = df_window.groupBy("payment_id").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты payment_id в окне + pending: {dups}"
    return f"OK: {df_window.count()} строк за окно [{s}; {e}) + pending - совпадает с источником, дубликатов нет"


# ---------------- INTERMEDIATE ----------------

def check_int_orders_with_payments(spark, load_date, s, e):
    path = P("intermediate/int_orders_with_payments")
    df = spark.read.parquet(path)
    assert len(df.columns) == 16, f"ожидалось 16 колонок, получено {len(df.columns)}"
    df_window = df.filter((F.col("order_date") >= s) & (F.col("order_date") < e))
    bounds = df_window.agg(F.min("order_date").alias("mn"), F.max("order_date").alias("mx")).first()
    assert bounds["mn"] >= s and bounds["mx"] < e, \
        f"данные окна выходят за границы [{s}; {e}): [{bounds['mn']}; {bounds['mx']}]"
    n_src = (spark.read.parquet(P("staging/stg_orders"))
             .filter((F.col("order_date") >= s) & (F.col("order_date") < e)).count())
    assert df_window.count() == n_src, \
        f"число строк за окно [{s}; {e}) разошлось: {n_src} -> {df_window.count()}"
    dups = df_window.groupBy("order_id", "order_date").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты (order_id, order_date) в окне: {dups}"
    row = df_window.filter(F.col("successful_payments_count") == 0).first()
    if row is not None:
        assert row["paid_amount"] == 0 and row["net_paid_amount"] == 0, "суммы должны быть 0"
        assert row["first_paid_at"] is None, "first_paid_at должен быть NULL"
        assert row["is_confirmed"] == False, "is_confirmed должен быть false"
    return f"OK: {df_window.count()} строк за окно [{s}; {e}) - совпадает со stg_orders, дубликатов нет"


# ---------------- MARTS ----------------

def check_fct_orders(spark, load_date, s, e):
    path = P("marts/fct_orders")
    df = spark.read.parquet(path)
    assert len(df.columns) == 12, f"ожидалось 12 колонок, получено {len(df.columns)}"
    df_window = df.filter((F.col("order_date") >= s) & (F.col("order_date") < e))
    bounds = df_window.agg(F.min("order_date").alias("mn"), F.max("order_date").alias("mx")).first()
    assert bounds["mn"] >= s and bounds["mx"] < e, \
        f"данные окна выходят за границы [{s}; {e}): [{bounds['mn']}; {bounds['mx']}]"
    df_int = (spark.read.parquet(P("intermediate/int_orders_with_payments"))
              .filter((F.col("order_date") >= s) & (F.col("order_date") < e)))
    n_expected = df_int.filter(F.col("is_test_order") == False).count()
    assert df_window.count() == n_expected, \
        f"ожидалось {n_expected} строк за окно, получено {df_window.count()}"
    dups = df_window.groupBy("order_id", "order_date").count().filter("count > 1").count()
    assert dups == 0, f"дубликаты (order_id, order_date) в окне: {dups}"
    return (f"OK: {df_window.count()} строк за окно [{s}; {e}) - "
            f"int_orders_with_payments без тестовых заказов, дубликатов нет")


def check_fct_orders_delta(spark, load_date, s, e):
    # В ноутбуке автопроверки для delta-марты нет: упрощённая проверка читаемости и границ окна.
    df = spark.read.format("delta").load(P("marts/fct_orders_delta"))
    df_window = df.filter((F.col("order_date") >= s) & (F.col("order_date") < e))
    n = df_window.count()
    assert n > 0, f"пусто окно [{s}; {e}) в fct_orders_delta"
    return f"OK: {n} строк за окно [{s}; {e}) (упрощённая проверка - в ноутбуке проверки delta нет)"


CHECKS = {
    "raw_customers":            (check_raw_customers, False),
    "raw_orders":               (check_raw_orders, False),
    "raw_payments":             (check_raw_payments, False),
    "raw_events":               (check_raw_events, False),
    "raw_products":             (check_raw_products, False),
    "raw_product_categories":   (check_raw_product_categories, False),
    "raw_order_items":          (check_raw_order_items, False),
    "stg_orders":         (check_stg_orders, False),
    "stg_payments":       (check_stg_payments, False),
    "int_orders_with_payments": (check_int_orders_with_payments, False),
    "fct_orders":               (check_fct_orders, False),
    "fct_orders_delta":         (check_fct_orders_delta, True),
}


def append_log(log_file, record):
    if not log_file:
        return
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", required=True, choices=sorted(CHECKS))
    parser.add_argument("--load-date", required=True)
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
    start_date = load_date - timedelta(days=3)

    fn, need_delta = CHECKS[args.check]
    spark = make_spark(delta=need_delta)
    spark.sparkContext.setLogLevel("ERROR")

    record = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "run_id": args.run_id,
        "load_date": args.load_date,
        "check": args.check,
    }
    try:
        record["status"] = "PASSED"
        record["message"] = fn(spark, load_date, start_date, load_date)
    except AssertionError as ex:
        record["status"] = "FAILED"
        record["message"] = str(ex)
    except Exception as ex:
        record["status"] = "ERROR"
        record["message"] = f"{type(ex).__name__}: {ex}"

    append_log(args.log_file, record)
    print(f"[{record['status']}] {args.check}: {record['message']}")
    spark.stop()
    # код возврата: 0 - прошло, 1 - проверка не прошла
    exit(0 if record["status"] == "PASSED" else 1)
