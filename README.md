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
    
    l_order_user  }o--|| h_order : h_order_pk
    l_order_user  }o--||  h_user : h_user_pk
    s_order_cost  }o--||  h_order : h_order_pk
    s_order_status  }o--||  h_order : h_order_pk
    s_user_names  }o--||  h_user : h_user_pk
```

```mermaid
erDiagram
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
    
    l_product_category  }o--||  h_category : h_category_pk
    l_product_category  }o--||  h_product : h_product_pk
    l_product_restaurant  }o--||  h_product : h_product_pk
    l_product_restaurant  }o--||  h_restaurant : h_restaurant_pk
    s_product_names  }o--||  h_product : h_product_pk
    s_restaurant_names  }o--||  h_restaurant : h_restaurant_pk
```

```mermaid
erDiagram
    l_order_product {
       uuid hk_order_product_pk PK
       uuid h_order_pk FK
       uuid h_product_pk FK
       timestamp load_dt
       varchar load_src
    }
    
    l_order_product  }o--||  h_order : h_order_pk
    l_order_product  }o--||  h_product : h_product_pk
```

### Common Data Marts (CDM)

CDM Общие витрины для заказчика.

```puml
@startuml

!theme plain
top to bottom direction
skinparam linetype ortho

class user_category_counters {
   user_id: uuid
   category_id: uuid
   category_name: varchar
   order_cnt: integer
   id: integer
}
class user_product_counters {
   user_id: uuid
   product_id: uuid
   product_name: varchar
   order_cnt: integer
   id: integer
}

@enduml
```

## Логика работы сервисов

### Service STG

### Service DDS

### Service CDM
