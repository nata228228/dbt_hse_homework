import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "de-course"
FCT_ORDERS_PATH = f"s3a://{BUCKET}/marts/fct_orders"

spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True, help="Дата запуска загрузки, ГГГГ-ММ-ДД")
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)

df_orders = (
    spark.read.parquet(
        f"s3a://{BUCKET}/intermediate/int_orders_with_payments"
    )
    .filter(
        (F.col("order_date") >= start_date)
        & (F.col("order_date") < load_date)
    )
)

df_fct_orders = (
    df_orders
    .filter(~F.col("is_test_order"))
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "ordered_at",
        "order_status",
        "order_amount",
        "paid_amount",
        "refunded_amount",
        "net_paid_amount",
        "successful_payments_count",
        "first_paid_at",
        "is_confirmed"
    )
)

print("Кол-во столбцов:", len(df_fct_orders.columns))
df_fct_orders.printSchema()

assert len(df_fct_orders.columns) == 12, \
    "Ошибка: должно быть 12 столбцов"

duplicates = (
    df_fct_orders
    .groupBy("order_id", "order_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Кол-во дубликатов:", duplicates)

assert duplicates == 0, \
    "Обнаружены дубликаты"

test_orders_count = df_orders.filter(
    F.col("is_test_order")
).count()

expected_count = df_orders.count() - test_orders_count
actual_count = df_fct_orders.count()

print("Кол-во тестовых заказов:", test_orders_count)
print("Ожидаемое кол-во строк:", expected_count)
print("Фактическое кол-во строк:", actual_count)

assert actual_count == expected_count, \
    "Неверное количество строк"
    
    
(
    df_fct_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(FCT_ORDERS_PATH)
)

print(f"Сохранено: {FCT_ORDERS_PATH}, строк: {actual_count}")

