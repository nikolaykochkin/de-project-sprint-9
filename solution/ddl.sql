DROP TABLE IF EXISTS cdm.user_product_counters;

CREATE TABLE IF NOT EXISTS cdm.user_product_counters
(
    id           SERIAL PRIMARY KEY,
    user_id      UUID    NOT NULL,
    product_id   UUID    NOT NULL,
    product_name VARCHAR NOT NULL,
    order_cnt    INT     NOT NULL CHECK ( order_cnt >= 0 ),
    UNIQUE (user_id, product_id)
);

DROP TABLE IF EXISTS cdm.user_category_counters;

CREATE TABLE IF NOT EXISTS cdm.user_category_counters
(
    id            SERIAL PRIMARY KEY,
    user_id       UUID    NOT NULL,
    category_id   UUID    NOT NULL,
    category_name VARCHAR NOT NULL,
    order_cnt     INT     NOT NULL CHECK ( order_cnt >= 0 ),
    UNIQUE (user_id, category_id)
);

DROP TABLE IF EXISTS stg.order_events;

CREATE TABLE IF NOT EXISTS stg.order_events
(
    id          SERIAL PRIMARY KEY,
    object_id   INT       NOT NULL UNIQUE,
    payload     JSON      NOT NULL,
    object_type VARCHAR   NOT NULL,
    sent_dttm   TIMESTAMP NOT NULL
);


DROP TABLE IF EXISTS dds.h_user;

CREATE TABLE IF NOT EXISTS dds.h_user
(
    h_user_pk UUID PRIMARY KEY,
    user_id   VARCHAR   NOT NULL UNIQUE,
    load_dt   TIMESTAMP NOT NULL,
    load_src  VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.h_product;

CREATE TABLE IF NOT EXISTS dds.h_product
(
    h_product_pk UUID PRIMARY KEY,
    product_id   VARCHAR   NOT NULL UNIQUE,
    load_dt      TIMESTAMP NOT NULL,
    load_src     VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.h_category;

CREATE TABLE IF NOT EXISTS dds.h_category
(
    h_category_pk UUID PRIMARY KEY,
    category_name VARCHAR   NOT NULL UNIQUE,
    load_dt       TIMESTAMP NOT NULL,
    load_src      VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.h_restaurant;

CREATE TABLE IF NOT EXISTS dds.h_restaurant
(
    h_restaurant_pk UUID PRIMARY KEY,
    restaurant_id   VARCHAR   NOT NULL UNIQUE,
    load_dt         TIMESTAMP NOT NULL,
    load_src        VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.h_order;

CREATE TABLE IF NOT EXISTS dds.h_order
(
    h_order_pk UUID PRIMARY KEY,
    order_id   INT       NOT NULL UNIQUE,
    order_dt   TIMESTAMP NOT NULL,
    load_dt    TIMESTAMP NOT NULL,
    load_src   VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.l_order_product;

CREATE TABLE IF NOT EXISTS dds.l_order_product
(
    hk_order_product_pk UUID PRIMARY KEY,
    h_order_pk          UUID      NOT NULL REFERENCES dds.h_order (h_order_pk),
    h_product_pk        UUID      NOT NULL REFERENCES dds.h_product (h_product_pk),
    load_dt             TIMESTAMP NOT NULL,
    load_src            VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.l_product_restaurant;

CREATE TABLE IF NOT EXISTS dds.l_product_restaurant
(
    hk_product_restaurant_pk UUID PRIMARY KEY,
    h_product_pk             UUID      NOT NULL REFERENCES dds.h_product (h_product_pk),
    h_restaurant_pk          UUID      NOT NULL REFERENCES dds.h_restaurant (h_restaurant_pk),
    load_dt                  TIMESTAMP NOT NULL,
    load_src                 VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.l_product_category;

CREATE TABLE IF NOT EXISTS dds.l_product_category
(
    hk_product_category_pk UUID PRIMARY KEY,
    h_product_pk           UUID      NOT NULL REFERENCES dds.h_product (h_product_pk),
    h_category_pk          UUID      NOT NULL REFERENCES dds.h_category (h_category_pk),
    load_dt                TIMESTAMP NOT NULL,
    load_src               VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.l_order_user;

CREATE TABLE IF NOT EXISTS dds.l_order_user
(
    hk_order_user_pk UUID PRIMARY KEY,
    h_order_pk       UUID      NOT NULL REFERENCES dds.h_order (h_order_pk),
    h_user_pk        UUID      NOT NULL REFERENCES dds.h_user (h_user_pk),
    load_dt          TIMESTAMP NOT NULL,
    load_src         VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.s_user_names;

CREATE TABLE IF NOT EXISTS dds.s_user_names
(
    hk_user_names_pk UUID PRIMARY KEY,
    h_user_pk        UUID      NOT NULL REFERENCES dds.h_user (h_user_pk),
    username         VARCHAR   NOT NULL,
    userlogin        VARCHAR   NOT NULL,
    load_dt          TIMESTAMP NOT NULL,
    load_src         VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.s_product_names;

CREATE TABLE IF NOT EXISTS dds.s_product_names
(
    hk_product_names_pk UUID PRIMARY KEY,
    h_product_pk        UUID      NOT NULL REFERENCES dds.h_product (h_product_pk),
    name                VARCHAR   NOT NULL,
    load_dt             TIMESTAMP NOT NULL,
    load_src            VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.s_restaurant_names;

CREATE TABLE IF NOT EXISTS dds.s_restaurant_names
(
    hk_restaurant_names_pk UUID PRIMARY KEY,
    h_restaurant_pk        UUID      NOT NULL REFERENCES dds.h_restaurant (h_restaurant_pk),
    name                   VARCHAR   NOT NULL,
    load_dt                TIMESTAMP NOT NULL,
    load_src               VARCHAR   NOT NULL
);

DROP TABLE IF EXISTS dds.s_order_cost;

CREATE TABLE IF NOT EXISTS dds.s_order_cost
(
    hk_order_cost_pk UUID PRIMARY KEY,
    h_order_pk       UUID           NOT NULL REFERENCES dds.h_order (h_order_pk),
    cost             DECIMAL(19, 5) NOT NULL CHECK ( cost >= 0 ),
    payment          DECIMAL(19, 5) NOT NULL CHECK ( payment >= 0 ),
    load_dt          TIMESTAMP      NOT NULL,
    load_src         VARCHAR        NOT NULL
);

DROP TABLE IF EXISTS dds.s_order_status;

CREATE TABLE IF NOT EXISTS dds.s_order_status
(
    hk_order_status_pk UUID PRIMARY KEY,
    h_order_pk         UUID      NOT NULL REFERENCES dds.h_order (h_order_pk),
    status             VARCHAR   NOT NULL,
    load_dt            TIMESTAMP NOT NULL,
    load_src           VARCHAR   NOT NULL
);
