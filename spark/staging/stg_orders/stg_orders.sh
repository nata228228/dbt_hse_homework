# Запуск загрузки с локальной машины:
# docker exec ubuntu-vm bash -c \
#   'cd /home/student/study/store_hse/spark/staging/stg_orders && bash stg_orders.sh --load-date 2026-03-05'

spark-submit \
  --master spark://spark-master:7077 \
  --name load-stg_orders \
  --deploy-mode client \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.dynamicAllocation.enabled=false \
  stg_orders.py "$@"
