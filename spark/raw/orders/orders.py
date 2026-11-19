import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql import SparkSession


PG = dict(
    host='postgres',
    port=5432,
    dbname=os.environ.get('POSTGRES_DB'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
)

BUCKET = 'de-course'
ORDERS_PATH = f"s3a://{BUCKET}/raw/orders"


def sink_has_data(spark: SparkSession, path: str) -> bool:
    """True, если в приёмнике уже есть данные."""
    try:
        return spark.read.parquet(path).limit(1).count() > 0
    except Exception:
        return False


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

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True, help="Дата запуска загрузки, ГГГГ-ММ-ДД")
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)

jdbc = dict(
    url=f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}",
    user=PG['user'],
    password=PG['password'],
    driver='org.postgresql.Driver',
)

if sink_has_data(spark, ORDERS_PATH):
    print(f"Приёмник не пуст — инкремент [{start_date} .. {load_date}]")
    where = f"""
        WHERE order_created_at >= '{start_date}'::timestamp
          AND order_created_at <  '{load_date}'::timestamp
    """
else:
    print("данных в приёмнике нет — полная загрузка таблицы")
    where = ""

# Читаем данные за 3 последних дня (учебное допущение, что данные могут изменяться в течение 3-х дней)
query = f"""
    SELECT order_id
         , customer_id
         , order_created_at
         , status
         , total_amount
    FROM public.orders
    {where}
"""

df_orders = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

# Партиционирование: order_created_dt = date(order_created_at)
df_orders = df_orders.withColumn("order_created_dt", F.to_date("order_created_at"))

# Перезапись: благодаря dynamic overwrite перезаписываются только 3 затронутые партиции

# Dynamic overwrite сохраняет исторические данные за пределами окна загрузки.
# При первой загрузке записываются все партиции, при повторной-- данные заменяются, не добавляются, так нет дубликатов.


(
    df_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_created_dt")
    .save(ORDERS_PATH)
)

print(f"Сохранено: {ORDERS_PATH}")
