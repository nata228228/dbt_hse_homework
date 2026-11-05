# store_hse: аналитический слой интернет-магазина

схема «звезда»
(staging, intermediate, marts). 

Документация проекта: dbt docs generate && dbt docs serve.

## Запуск

```bash
docker compose down -v && docker compose up -d
docker compose logs loader
docker compose run --rm dbt deps 
docker compose run --rm dbt build
```

## Модель данных

```mermaid
erDiagram
    dim_customers ||--o{ fct_orders : "customer_id"
    dim_customers ||--o{ fct_order_items : "customer_id"
    dim_products  ||--o{ fct_order_items : "product_id"
    dim_dates     ||--o{ fct_orders : "order_date"
    dim_dates     ||--o{ fct_order_items : "order_date"
    fct_orders    ||--|{ fct_order_items : "order_id"

    dim_customers {
        int customer_id PK
        string city
        string acquisition_channel
        bool is_buyer
    }
    dim_products {
        int product_id PK
        string product_name
        string brand
        string root_category_name
    }
    dim_dates {
        date date_day PK
        int calendar_year
        bool is_weekend
    }
    fct_orders {
        int order_id PK
        int customer_id FK
        date order_date FK
        numeric order_amount
        int items_count
    }
    fct_order_items {
        string order_item_id PK
        int order_id FK
        int product_id FK
        int customer_id FK
        date order_date FK
        numeric line_amount
    }
```

### Витрины

| Витрина           | Зернистость                       | Бизнес-вопрос                                                                   |
|:------------------|:----------------------------------|:--------------------------------------------------------------------------------|
| fct_orders      | один заказ                        | Выручка и средний чек по периодам, городам и каналам; доля возвратов            |
| fct_order_items | один товар в заказе               | Выручка по месяцам и категориям; какие бренды чаще возвращают                   |
| dim_customers   | один клиент, включая не купивших  | Доля покупателей по каналам привлечения; средний чек по городам; первая покупка |
| dim_products    | один товар                        | Срезы по бренду и иерархии категорий (категория, родитель, корень)              |
| dim_dates       | один день                         | Группировка по году, кварталу, месяцу, дню недели и выходным                    |

### Хранение в Greenplum

| Витрина           | Распределение                | Формат      | Почему так                                                                                       |
|:------------------|:-----------------------------|:------------|:-------------------------------------------------------------------------------------------------|
| fct_orders      | distributed by (order_id)  | колоночный   | Ключ уникален и равномерен; совпадает с ключом fct_order_items, JOIN по order_id идёт локально |
| fct_order_items | distributed by (order_id)  | колоночный  | Тот же ключ, что у fct_orders; таблица широкая, отчёты читают 2-3 столбца                      |
| dim_customers   | distributed replicated     | строковый   | Маленькое измерение: копия на каждом сегменте, JOIN с фактами без пересылки строк                |
| dim_products    | distributed replicated     | строковый   | То же                                                                                            |
| dim_dates       | distributed replicated     | строковый   | То же                                                                                            |

Исходные таблицы в raw в Greenplum по умолчанию распределены по случайному ключу (DISTRIBUTED RANDOMLY).
Для фактов fct_orders и fct_order_items мы использовали одинаковый ключ распределения — order_id.

Если ключи были бы разные:
Если бы fct_orders была распределена по order_id, а fct_order_items — по product_id (или вообще случайно), то при их соединении Greenplum пришлось бы пересылать данные между сегментами по сети. Это сильно замедляет запросы. Одинаковый ключ order_id гарантирует, что данные для одного заказа лежат на одном сегменте, и JOIN происходит локально и мгновенно.

### Lineage

![lineage](docs/lineage.png)

## Тесты

Generic-тесты: unique, not_null, relationships (факты к измерениям), accepted_values
(статусы заказа и платежа, корневая категория) и собственный non_negative.
Singular-тесты на бизнес-логику лежат в tests/.


## Дополнительное задание

Второй вариант: подключён пакет dbt_utils (`packages.yml`), календарь dim_dates построен
через dbt_utils.date_spine.