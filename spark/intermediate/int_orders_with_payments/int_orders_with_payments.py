import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "de-course"
INT_ORDERS_PATH = f"s3a://{BUCKET}/intermediate/int_orders_with_payments"

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
parser.add_argument(
    "--load-date",
    required=True,
    help="Дата запуска загрузки, ГГГГ-ММ-ДД"
)
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)

df_orders = (
    spark.read.parquet(f"s3a://{BUCKET}/staging/stg_orders")
    .filter(
        (F.col("order_date") >= start_date)
        & (F.col("order_date") < load_date)
    )
)

df_payments = (
    spark.read.parquet(f"s3a://{BUCKET}/staging/stg_payments")
    .filter(
        ((F.col("paid_dt") >= start_date) & (F.col("paid_dt") < load_date))
        | F.col("paid_dt").isNull()
    )
)


df_order_payments = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count(
            F.when(
                (F.col("payment_status") == "success")
                & (~F.col("is_refund")),
                1
            )
        ).alias("successful_payments_count"),

        F.coalesce(
            F.sum(
                F.when(
                    (F.col("payment_status") == "success")
                    & (~F.col("is_refund")),
                    F.col("amount")
                )
            ),
         F.lit(0)
        ).alias("paid_amount"),
        
        F.coalesce(
            -F.sum(
                F.when(
                    F.col("is_refund"),
                    F.col("amount")
                )
            ),
            F.lit(0)
        ).alias("refunded_amount"),
        
        F.min(
            F.when(
                (F.col("payment_status") == "success")
                & (~F.col("is_refund")),
                F.col("paid_at")
            )
        ).alias("first_paid_at"),
        F.count(
            F.when(F.col("payment_status") == "success", 1)
        ).alias("success_operations_count"),

        F.count(
            F.when(F.col("payment_status") == "failed", 1)
        ).alias("failed_operations_count"),

        F.count(
            F.when(F.col("payment_status") == "pending", 1)
        ).alias("pending_operations_count")
    )
)

df_joined = (
    df_orders.alias("o")
    .join(
        df_order_payments.alias("p"),
        F.col("o.order_id") == F.col("p.order_id"),
        "left"
    )
)

df_int_orders = (
    df_joined
    .select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.customer_id"),
        F.col("o.ordered_at"),
        F.col("o.order_date"),
        F.col("o.order_status"),
        F.col("o.order_amount"),
        F.col("o.is_test_order"),
        F.coalesce(
            F.col("p.successful_payments_count"),
            F.lit(0)
        ).alias("successful_payments_count"),

        F.coalesce(
            F.col("p.paid_amount"),
            F.lit(0)
        ).alias("paid_amount"),

        F.coalesce(
            F.col("p.refunded_amount"),
            F.lit(0)
        ).alias("refunded_amount"),
        (
            F.coalesce(F.col("p.paid_amount"), F.lit(0))
            - F.coalesce(F.col("p.refunded_amount"), F.lit(0))
        ).alias("net_paid_amount"),
        
        F.col("p.first_paid_at"),

        (
            (F.col("o.order_status") == "paid")
            & (
                F.coalesce(
                    F.col("p.successful_payments_count"),
                    F.lit(0)
                ) > 0
            )
        ).alias("is_confirmed"),
        
        F.coalesce(
            F.col("p.success_operations_count"),
            F.lit(0)
        ).alias("success_operations_count"),

        F.coalesce(
            F.col("p.failed_operations_count"),
            F.lit(0)
        ).alias("failed_operations_count"),

        F.coalesce(
        F.col("p.pending_operations_count"),
            F.lit(0)
        ).alias("pending_operations_count")
    )
)

print("Кол-во столбцов", len(df_int_orders.columns))
df_int_orders.printSchema()


duplicates = (
    df_int_orders
    .groupBy("order_id", "order_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Кол-во дубликатов", duplicates)


print("Заказы без успешных платежей")

df_int_orders.filter(
    F.col("successful_payments_count") == 0
).select(
    "order_id",
    "successful_payments_count",
    "paid_amount",
    "net_paid_amount",
    "first_paid_at",
    "is_confirmed"
).show(truncate=False)

print("Заказы, у которых нет платежей")

df_orders.join(
    df_payments.select("order_id").distinct(),
    on="order_id",
    how="left_anti"
).select("order_id").show()

assert df_int_orders.count() == df_orders.count(), \
    "Ошибка!!!! Кол-во строк не совпадает с кол-вом заказов"

(
    df_int_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(INT_ORDERS_PATH)
)

print(f"Сохранено: {INT_ORDERS_PATH}, строк: {df_int_orders.count()}")