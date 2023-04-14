# Проектная работа по облачным технологиям

## Бизнес требования

Разработать облачный DWH для тегирования пользователей приложения - для повышения эффективности рекламных акций.

Маркетинг будет проводить кампании, заточенные под гостей с определённым тегом. Все расчёты ведутся только по заказам со
статусом CLOSED. В каждом заказе есть категория. Для этих категорий будут заведены счётчики с конкретными порогами. При
превышении порога гостю записывается тег.

## Функциональные требования

Входные данные: 
1. Информация по заказам — это потоковые данные, которые будут передаваться через брокер сообщений в
формате JSON.
2. Справочные данные — извлекаются из хранилища ключ-значение.

Выходные данные: 
- Первая витрина — счётчик заказов по блюдам; 
- вторая — счётчик заказов по категориям товаров.

## Нефункциональные требования

- Нагрузка на систему заказов — 5 заказов в минуту.  
- На старте загрузят историю за неделю — 50 000 заказов сразу.
- Обеспечить идемпотентность обработки сообщений из брокера.
- Суммарный бюджет на весь проект не должен превышать 5000 рублей в месяц.
- Использовать технологии, которые позволят легко масштабировать сервис в будущем.
- Нужно оставить возможность переходить из одного облака в другое.

## Технологии

- Kafka - брокер сообщений
- Redis - хранение справочных данных
- PostgreSQL - DWH
- YandexCloud - облачный провайдер
- DataLens - визуализация данных
- Python - обработка данных
- Kubernetes - назначение сервисов

## Общая схема системы

![schema.png](img%2Fschema.png)

## Описание данных

### Staging-слой (STG)

STG Слой с исходными данными as is — источник правды.

```mermaid
erDiagram
    order_events {
       int id PK
       int object_id UK
       json payload
       varchar object_type
       timestamp sent_dttm
    }
```

### Detail Data Store (DDS)

DDS Слой детализированных данных для сбора в удобном для управления виде.

Модель DDS Data Vault.

```mermaid
erDiagram
    h_order {
       uuid h_order_pk PK
       int order_id UK
       timestamp order_dt
       timestamp load_dt
       varchar load_src
    }
    
    h_user {
       uuid h_user_pk PK
       varchar user_id UK
       timestamp load_dt
       varchar load_src
    }
    
    l_order_user {
       uuid hk_order_user_pk PK
       uuid h_order_pk FK
       uuid h_user_pk FK
       timestamp load_dt
       varchar load_src
    }
    
    s_order_cost {
       uuid hk_order_cost_pk PK
       uuid h_order_pk FK
       numeric cost
       numeric payment
       timestamp load_dt
       varchar load_src
    }
    
    s_order_status {
       uuid hk_order_status_pk PK
       uuid h_order_pk FK
       varchar status
       timestamp load_dt
       varchar load_src
    }
    
    s_user_names {
       uuid hk_user_names_pk PK
       uuid h_user_pk FK
       varchar username
       varchar userlogin
       timestamp load_dt
       varchar load_src
    }
    
    h_category {
       uuid h_category_pk PK
       varchar category_name UK
       timestamp load_dt
       varchar load_src
    }
    
    h_product {
       uuid h_product_pk PK
       varchar product_id UK
       timestamp load_dt
       varchar load_src
    }
    
    h_restaurant {
       uuid h_restaurant_pk PK
       varchar restaurant_id UK
       timestamp load_dt
       varchar load_src
    }
    
    l_product_category {
       uuid hk_product_category_pk PK
       uuid h_product_pk FK
       uuid h_category_pk FK
       timestamp load_dt
       varchar load_src
    }
    
    l_product_restaurant {
       uuid hk_product_restaurant_pk PK
       uuid h_product_pk FK
       uuid h_restaurant_pk FK
       timestamp load_dt
       varchar load_src
    }
    
    s_product_names {
       uuid hk_product_names_pk PK
       uuid h_product_pk FK
       varchar name
       timestamp load_dt
       varchar load_src
    }
    
    s_restaurant_names {
       uuid hk_restaurant_names_pk PK
       uuid h_restaurant_pk FK
       varchar name
       timestamp load_dt
       varchar load_src
    }
    
    l_order_product {
       uuid hk_order_product_pk PK
       uuid h_order_pk FK
       uuid h_product_pk FK
       timestamp load_dt
       varchar load_src
    }

    l_order_product }o--|| h_product: h_product_pk
    l_order_user }o--|| h_order: h_order_pk
    l_product_category }o--|| h_category: h_category_pk
    l_product_restaurant }o--|| h_product: h_product_pk
    l_product_restaurant }o--|| h_restaurant: h_restaurant_pk
    s_order_cost }o--|| h_order: h_order_pk
    s_order_status }o--|| h_order: h_order_pk
    s_user_names }o--|| h_user: h_user_pk

    h_order ||--o{ l_order_product: h_order_pk
    h_user ||--o{ l_order_user: h_user_pk
    h_product ||--o{ l_product_category: h_product_pk
    h_product ||--o{ s_product_names: h_product_pk
    h_restaurant ||--o{ s_restaurant_names: h_restaurant_pk
```

### Common Data Marts (CDM)

CDM Общие витрины для заказчика.

```mermaid
erDiagram
    user_category_counters {
        int id PK
        uuid user_id UK
        uuid category_id UK
        varchar category_name
        int order_cnt
    }

    user_product_counters {
        int id PK
        uuid user_id UK
        uuid product_id UK
        varchar product_name
        int order_cnt
    }
```

## Логика работы сервисов

### STG-Service

**Registry link:** cr.yandex/crppomhsg1o5elrk760j/stg_service

#### Пример входного сообщения из order-service_orders

```json
{
  "object_id": 1371198,
  "object_type": "order",
  "sent_dttm": "2023-04-08 20:27:36",
  "payload": {
    "restaurant": {
      "id": "ef8c42c19b7518a9aebec106"
    },
    "date": "2023-04-01 20:45:43",
    "user": {
      "id": "626a81ce9a8cd1920641e29c"
    },
    "order_items": [
      {
        "id": "aeca4d08abab78869c20128c",
        "name": "Рис с лимоном",
        "price": 175,
        "quantity": 5
      }
    ],
    "bonus_payment": 0,
    "cost": 875,
    "payment": 875,
    "bonus_grant": 0,
    "statuses": [
      {
        "status": "CLOSED",
        "dttm": "2023-04-01 20:45:43"
      },
      {
        "status": "DELIVERING",
        "dttm": "2023-04-01 20:12:14"
      },
      {
        "status": "COOKING",
        "dttm": "2023-04-01 19:14:45"
      },
      {
        "status": "OPEN",
        "dttm": "2023-04-01 18:55:52"
      }
    ],
    "final_status": "CLOSED",
    "update_ts": "2023-04-01 20:45:43"
  }
}
```

#### Пример выходного сообщения в stg-service-orders

```json
{
  "object_id": 322519,
  "object_type": "order",
  "payload": {
    "id": 322519,
    "date": "2022-11-19 16:06:36",
    "cost": 300,
    "payment": 300,
    "status": "CLOSED",
    "restaurant": {
      "id": "626a81cfefa404208fe9abae",
      "name": "Кофейня №1"
    },
    "user": {
      "id": "626a81ce9a8cd1920641e296",
      "name": "Котова Ольга Вениаминовна"
    },
    "products": [
      {
        "id": "6276e8cd0cf48b4cded00878",
        "price": 180,
        "quantity": 1,
        "name": "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ",
        "category": "Выпечка"
      }
    ]
  }
}
```

#### Порядок действий при обработке сообщения

```mermaid
sequenceDiagram
    participant kafka as Kafka
    participant service as STG-Service
    participant dwh as DWH.STG
    participant redis as Redis
    kafka ->> service : Получить сообщение из<br/>топика order-service_orders
    service ->> dwh : Сложить сообщение as-is<br/>в STG слой по логике upsert
    redis ->> service : Получить user по<br/>user_id из сообщения
    redis ->> service : Получить restaurant по<br/>restaurant_id из сообщения
    loop Для каждого product_id в сообщении
        service ->> service : Найти в restaurant.menu продукт<br/>по product_id и получить его категорию
    end
    service ->> service : Сформировать выходное сообщение
    service ->> kafka : Отправить выходное сообщение<br/>в топик stg-service-orders
```

### DDS-Service

**Registry link:** cr.yandex/crppomhsg1o5elrk760j/dds_service

#### Пример входного сообщения из stg-service-orders

```json
{
  "object_id": 322519,
  "object_type": "order",
  "payload": {
    "id": 322519,
    "date": "2022-11-19 16:06:36",
    "cost": 300,
    "payment": 300,
    "status": "CLOSED",
    "restaurant": {
      "id": "626a81cfefa404208fe9abae",
      "name": "Кофейня №1"
    },
    "user": {
      "id": "626a81ce9a8cd1920641e296",
      "name": "Котова Ольга Вениаминовна"
    },
    "products": [
      {
        "id": "6276e8cd0cf48b4cded00878",
        "price": 180,
        "quantity": 1,
        "name": "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ",
        "category": "Выпечка"
      }
    ]
  }
}
```

#### Пример выходного сообщения в cdm-service-stats

```json
[
  {
    "user_id": "47044875-5c7b-448e-830a-bc6d13fe11da",
    "product_id": "b7d5264c-2b37-4666-be51-23f80756892c",
    "product_name": "Салат Тбилисо",
    "category_id": "8690bd24-85e0-4eab-8091-26c81995cd1c",
    "category_name": "Салаты",
    "order_cnt": 1
  }
]
```

#### Порядок действий при обработке сообщения

```mermaid
sequenceDiagram
    participant kafka as Kafka
    participant service as DDS-Service
    participant dwh as DWH.DDS
    kafka ->> service : Получить сообщение из<br/>топика stg-service-orders
    service ->> dwh : Создать временные таблицы для заказа и продуктов
    service ->> dwh : Загрузить данные заказа во временную таблицу
    service ->> dwh : Загрузить данные о продуктах во временную таблицу
    service ->> dwh : Загрузить отсутствующие записи хабов
    service ->> dwh : Дополнить временные таблицы ключами хабов
    service ->> dwh : Загрузить отсутствующие линки
    service ->> dwh : Загрузить сателлиты по логике upsert
    dwh ->> service : Сформировать статистику по всем закрытым заказам пользователя<br/>в разрезе продуктов и категорий
    service ->> service : Сформировать из статистики выходное сообщение
    service ->> kafka : Отправить выходное сообщение<br/>в топик cdm-service-stats
```

### CDM-Service

**Registry link:** cr.yandex/crppomhsg1o5elrk760j/cdm_service

#### Пример входного сообщения из cdm-service-stats

```json
[
  {
    "user_id": "47044875-5c7b-448e-830a-bc6d13fe11da",
    "product_id": "b7d5264c-2b37-4666-be51-23f80756892c",
    "product_name": "Салат Тбилисо",
    "category_id": "8690bd24-85e0-4eab-8091-26c81995cd1c",
    "category_name": "Салаты",
    "order_cnt": 1
  }
]
```

#### Порядок действий при обработке сообщения

```mermaid
sequenceDiagram
    participant kafka as Kafka
    participant service as CDM-Service
    participant dwh as DWH.CDM
    kafka ->> service : Получить сообщение из<br/>топика cdm-service-stats
    service ->> dwh : Создать временную таблицу для статистики
    service ->> dwh : Загрузить данные из сообщения во временную таблицу
    service ->> dwh : Загрузить данные из временной таблицы в витрины по логике upsert
```

## Dashboard

[Популярность блюд](https://datalens.yandex/2earrr8c3dl8s)

![Dashboard.png](img%2FDashboard.png)
