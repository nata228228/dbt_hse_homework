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
PRODUCT_CATEGORIES_PATH = f"s3a://{BUCKET}/raw/product_categories"

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

# Полный снимок справочника product_category

# WHERE не нужен, тк загружается все категории
# Партиционирование не требуется, тк справочник каждый раз полностью перезаписывается.
# Условие «данные меняются до 3 дней»(окно в 3 дня) не применяется, тк загрузка не инкрементальная.
# Для этой загрузки проверка sink_has_data не нужна, тк данные целиком перезаписываются на каждом запуске программы.


query = """
    SELECT category_id
         , parent_category_id
         , category_name
    FROM public.product_categories
"""

df_product_categories = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

# Полная перезапись: overwrite без partitionBy
(
    df_product_categories.write
    .format("parquet")
    .mode("overwrite")
    .save(PRODUCT_CATEGORIES_PATH)
)

print(f"Сохранено: {PRODUCT_CATEGORIES_PATH}, строк: {df_product_categories.count()}")
