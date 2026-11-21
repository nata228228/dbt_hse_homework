#!/usr/bin/env bash
# Обёртка: заходит в контейнер ubuntu-vm и запускает оркестратор.
#
# Использование:
#   bash runner.sh                     # два прогона: 2026-03-05, затем 2026-03-06
#   bash runner.sh 2026-03-05 [2026-03-06 ...]   # прогоны по указанным датам по порядку
#
# Каждая дата - отдельный полный запуск пайплайна (raw -> staging -> intermediate -> marts).
# Лог общий: checks.log (записи различаются по run_id и load_date).

DATES=("$@")
if [[ ${#DATES[@]} -eq 0 ]]; then
  DATES=(2026-03-05 2026-03-06)
fi

FAILED_DATES=()
for LOAD_DATE in "${DATES[@]}"; do
  echo "=== RUNNER: запуск пайплайна за $LOAD_DATE ==="
  if docker exec ubuntu-vm bash -c "cd /home/student/study/store_hse/spark_check && bash run_check.sh --load-date $LOAD_DATE --log-file /home/student/study/store_hse/spark_check/check.log"; then
    echo "=== RUNNER: $LOAD_DATE - все проверки пройдены ==="
  else
    echo "=== RUNNER: $LOAD_DATE - есть FAIL, см. checks.log ==="
    FAILED_DATES+=("$LOAD_DATE")
  fi
done

if [[ ${#FAILED_DATES[@]} -eq 0 ]]; then
  echo "RUNNER FINISHED: все прогоны (${DATES[*]}) успешно завершены"
  exit 0
else
  echo "RUNNER FINISHED: проблемные прогоны: ${FAILED_DATES[*]}"
  exit 1
fi
