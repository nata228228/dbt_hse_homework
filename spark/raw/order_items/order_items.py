import os

from pyspark.sql import SparkSession


PG = dict(
    host='postgres',
    port=5432,
    dbname=os.environ.get('POSTGRES_DB'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
)

BUCKET = 'de-course'
ORDER_ITEMS_PATH = f"s3a://{BUCKET}/raw/order_items"

spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

jdbc = dict(
    url=f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}",
    user=PG['user'],
    password=PG['password'],
    driver='org.postgresql.Driver',
)

# Полная загрузка таблицы позиций заказов order_items.

# В таблице нет столбца с датой, поэтому непонятно, какие записи относятся к трёхдневному окну загрузки.
# WHERE по дате не нужен, тк перечитываем всю таблицу.
# Партиционирование не требуется, тк таблица каждый раз полностью перезаписывается.
# Для этой загрузки проверка sink_has_data не нужна, тк данные целиком перезаписываются на каждом запуске программы.


query = """
    SELECT order_id
         , product_id
         , quantity
         , unit_price
         , discount_amount
    FROM public.order_items
"""

df_order_items = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

# Полная перезапись: overwrite без partitionBy
(
    df_order_items.write
    .format("parquet")
    .mode("overwrite")
    .save(ORDER_ITEMS_PATH)
)

print(f"Сохранено: {ORDER_ITEMS_PATH}, строк: {df_order_items.count()}")
