# Запуск загрузки с локальной машины:
# docker exec ubuntu-vm bash -c \
#   'cd /home/student/study/store_hse/spark/raw/customers && bash customers.sh'

spark-submit \
  --master spark://spark-master:7077 \
  --name load-customers \
  --deploy-mode client \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.dynamicAllocation.enabled=false \
  customers.py
