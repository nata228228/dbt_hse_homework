#!/usr/bin/env bash
# Оркестратор пайплайна: raw -> staging -> intermediate -> marts.
# Каждый этап: spark-submit лоадера, затем автопроверка из ноутбука (check.py) с записью в лог.
#
# Использование (внутри контейнера, где доступен spark-submit и переменные MINIO_*/POSTGRES_*):
#   bash run_pipeline.sh --load-date 2026-03-05 [--log-file /home/student/study/store_hse/spark_check/check.log] [--skip-delta]
#
# Структура каталогов не меняется: скрипт лежит рядом с деревом study/store_hse/spark.

set -u  # без -e: падение одного шага логируем и решаем сами (see SHOULD_FAIL)

BASE_DIR="/home/student/study/store_hse"
SPARK_ROOT="$BASE_DIR/spark"
LOAD_DATE=""
LOG_FILE="$BASE_DIR/spark_check/check.log"
SKIP_DELTA=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --load-date)  LOAD_DATE="$2"; shift 2 ;;
    --log-file)   LOG_FILE="$2";  shift 2 ;;
    --skip-delta) SKIP_DELTA=1;   shift ;;
    *) echo "Неизвестный аргумент: $1"; exit 2 ;;
  esac
done

[[ -z "$LOAD_DATE" ]] && { echo "Обязателен --load-date ГГГГ-ММ-ДД"; exit 2; }

RUN_ID="$(date +%Y%m%d_%H%M%S)"
CHECKS_PY="$BASE_DIR/spark_check/check.py"
SPARK_SUBMIT="$(command -v spark-submit || echo /opt/spark/bin/spark-submit)"

log()   { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE"; }
logsep(){ log "------------------------------------------------------------"; }

# Запуск проверки. Возвращает 0/1 по статусу.
run_check() {
  local check="$1"
  "$SPARK_SUBMIT" \
    --master spark://spark-master:7077 \
    --name "check-$check" \
    --deploy-mode client \
    --conf spark.executor.instances=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.driver.memory=1g \
    --conf spark.dynamicAllocation.enabled=false \
    "$CHECKS_PY" --check "$check" --load-date "$LOAD_DATE" --log-file "$LOG_FILE" --run-id "$RUN_ID"
  return $?
}

# Запуск лоадера: run_step <имя> <dir> <sh> [args...]
FAILED=0
run_step() {
  local name="$1" dir="$2" sh="$3"; shift 3
  logsep
  log "STEP $name: запуск $sh $*"
  if (cd "$SPARK_ROOT/$dir" && bash "$sh" "$@"); then
    log "STEP $name: загрузка OK"
    if run_check "$name"; then
      log "STEP $name: проверка PASSED"
    else
      log "STEP $name: проверка FAILED - см. запись [FAILED] в логе"
      FAILED=1
    fi
  else
    log "STEP $name: загрузка FAILED (код $?)"
    echo "{\"ts\":\"$(date '+%F %T')\",\"run_id\":\"$RUN_ID\",\"load_date\":\"$LOAD_DATE\",\"check\":\"$name\",\"status\":\"ERROR\",\"message\":\"загрузка $sh завершилась с ошибкой\"}" >> "$LOG_FILE"
    FAILED=1
  fi
}

log "CHECKS START run_id=$RUN_ID load_date=$LOAD_DATE log=$LOG_FILE"

# ---------- RAW ----------
logsep; log "LAYER: RAW"
run_step raw_customers            raw/customers            customers.sh
run_step raw_orders               raw/orders               orders.sh            --load-date "$LOAD_DATE"
run_step raw_payments             raw/payments             payments.sh          --load-date "$LOAD_DATE"
run_step raw_events               raw/events               events.sh            --load-date "$LOAD_DATE"
run_step raw_products             raw/products             products.sh
run_step raw_product_categories   raw/product_categories   product_categories.sh
run_step raw_order_items          raw/order_items          order_items.sh

# ---------- STAGING ----------
logsep; log "LAYER: STAGING"
run_step stg_orders         staging/stg_orders         stg_orders.sh         --load-date "$LOAD_DATE"
run_step stg_payments       staging/stg_payments       stg_payments.sh       --load-date "$LOAD_DATE"

# ---------- INTERMEDIATE ----------
logsep; log "LAYER: INTERMEDIATE"
run_step int_orders_with_payments intermediate/int_orders_with_payments int_orders_with_payments.sh --load-date "$LOAD_DATE"

# ---------- MARTS ----------
logsep; log "LAYER: MARTS"
run_step fct_orders               marts/fct_orders         fct_orders.sh        --load-date "$LOAD_DATE"
# if [[ "$SKIP_DELTA" -eq 0 ]]; then
#   run_step fct_orders_delta       marts/fct_orders_delta   fct_orders_delta.sh  --load-date "$LOAD_DATE"
# else
#   log "STEP fct_orders_delta: пропущен (--skip-delta)"
# fi

logsep
if [[ "$FAILED" -eq 0 ]]; then
  log "CHECKS FINISHED run_id=$RUN_ID: все проверки пройдены"
else
  log "CHECKS FINISHED run_id=$RUN_ID: есть FAIL - см. check.log"
fi
exit $FAILED
