import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = 'de-course'
STG_PAYMENTS_PATH = f"s3a://{BUCKET}/staging/stg_payments"


spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # перезапись только затронутых партиций

    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True, help="Дата запуска загрузки, ГГГГ-ММ-ДД")
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)


# source: raw/payments (партиционирован по paid_dt), partition pruning по окну инкремента.
# ⚠️ pending-платежи (paid_dt IS NULL -> __HIVE_DEFAULT_PARTITION__) перечитываем ВСЕГДА
df_payments = (
    spark.read.parquet(f"s3a://{BUCKET}/raw/payments")
    .filter(
        ((F.col("paid_dt") >= start_date) & (F.col("paid_dt") < load_date))
        | F.col("paid_dt").isNull()
    )
)

df_stg_payments = (
    df_payments.select(
        F.col("payment_id"),
        F.col("order_id"),
        F.col("paid_at"),
        F.col("payment_status"),
        F.col("amount"),
        ((F.col("payment_status") == "success") & (F.col("amount") < 0)).alias("is_refund"),
    )
)

# Колонка партиционирования: paid_at IS NULL -> партиция __HIVE_DEFAULT_PARTITION__
df_stg_payments = df_stg_payments.withColumn("paid_dt", F.to_date("paid_at"))

# dynamic overwrite перезапишет только партиции окна (+ default-партицию)
(
    df_stg_payments.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("paid_dt")
    .save(STG_PAYMENTS_PATH)
)

print(f"Сохранено: {STG_PAYMENTS_PATH}, строк: {df_stg_payments.count()}")
print(f"Строк в RAW за то же окно (включая pending): {df_payments.count()}")

print("Схема STAGING payments:")
df_stg_payments.printSchema()


pending_count = df_stg_payments.filter(F.col("paid_dt").isNull()).count()
print(f"Количество pending-платежей с NULL-датой: {pending_count}")
