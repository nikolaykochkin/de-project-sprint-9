import uuid
from typing import Dict, List

from lib.pg.pg_connect import PgConnect
from psycopg import Cursor
from psycopg.rows import dict_row

from .model import InputMessage, Order, User


class DdsRepository:
    def __init__(self, db: PgConnect, final_status: str, load_src: str) -> None:
        self._db = db
        self._final_status = final_status
        self._load_src = load_src

    def save_message(self, message: InputMessage) -> None:
        order = message.payload

        message_id = uuid.uuid4().hex
        order_temp_table = f"temp_order_{message_id}"
        items_temp_table = f"temp_order_items_{message_id}"
        h_order_temp_table = f"h_temp_order_{message_id}"
        h_items_temp_table = f"h_temp_order_items_{message_id}"

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                self.__create_temp_tables(cur, order_temp_table, items_temp_table)
                self.__load_order_temp_table(cur, order_temp_table, order)
                self.__load_items_temp_table(cur, items_temp_table, order)
                self.__load_hubs(cur, order_temp_table, items_temp_table)
                self.__load_h_temp_tables(cur, order_temp_table, items_temp_table, h_order_temp_table,
                                          h_items_temp_table)
                self.__load_links(cur, h_order_temp_table, h_items_temp_table)
                self.__load_satellites(cur, h_order_temp_table, h_items_temp_table)

    def get_user_stats(self, user: User) -> List[Dict]:
        with self._db.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                        SELECT uopc.user_id           AS user_id,
                               uopc.h_product_pk      AS product_id,
                               spn.name               AS product_name,
                               uopc.h_category_pk     AS category_id,
                               hc.category_name       AS category_name,
                               COUNT(uopc.h_order_pk) AS order_cnt
                        FROM (SELECT hu.h_user_pk      AS user_id,
                                     lou.h_order_pk    AS h_order_pk,
                                     lop.h_product_pk  AS h_product_pk,
                                     lpc.h_category_pk AS h_category_pk
                              FROM dds.h_user AS hu
                                       LEFT JOIN dds.l_order_user AS lou ON hu.h_user_pk = lou.h_user_pk
                                       LEFT JOIN dds.s_order_status AS sos ON lou.h_order_pk = sos.h_order_pk
                                       LEFT JOIN dds.l_order_product AS lop ON lou.h_order_pk = lop.h_order_pk
                                       LEFT JOIN dds.l_product_category AS lpc ON lop.h_product_pk = lpc.h_product_pk
                              WHERE hu.user_id = %(user_id)s
                                AND sos.status = %(final_status)s) AS uopc
                                 LEFT JOIN dds.s_product_names AS spn ON uopc.h_product_pk = spn.h_product_pk
                                 LEFT JOIN dds.h_category AS hc ON uopc.h_category_pk = hc.h_category_pk
                        GROUP BY user_id, product_id, product_name, category_id, category_name;
                    """,
                    {
                        "user_id": user.id,
                        "final_status": self._final_status
                    }
                )
                return cur.fetchall()

    def __create_temp_tables(self, cur: Cursor, order_temp_table: str, items_temp_table: str) -> None:
        cur.execute(
            f"""
                CREATE TEMP TABLE {order_temp_table}
                (
                    order_id        INT,
                    order_date      TIMESTAMP,
                    order_cost      NUMERIC(19, 5),
                    order_payment   NUMERIC(19, 5),
                    order_status    VARCHAR,
                    user_id         VARCHAR,
                    user_name       VARCHAR,
                    user_login      VARCHAR,
                    restaurant_id   VARCHAR,
                    restaurant_name VARCHAR,
                    load_src        VARCHAR
                )
                    ON COMMIT DROP;
        
                CREATE TEMP TABLE {items_temp_table}
                (
                    order_id         INT,
                    restaurant_id    VARCHAR,
                    product_id       VARCHAR,
                    product_name     VARCHAR,
                    product_category VARCHAR,
                    load_src         VARCHAR
                )
                    ON COMMIT DROP;
            """
        )

    def __load_order_temp_table(self, cur: Cursor, order_temp_table: str, order: Order) -> None:
        cur.execute(
            f"""
                INSERT INTO {order_temp_table} 
                (order_id, order_date, order_cost, order_payment, order_status, user_id, user_name, user_login, 
                    restaurant_id, restaurant_name, load_src)
                VALUES 
                (
                    %(order_id)s,
                    %(order_date)s,
                    %(order_cost)s,
                    %(order_payment)s,
                    %(order_status)s,
                    %(user_id)s,
                    %(user_name)s,
                    %(user_login)s,
                    %(restaurant_id)s,
                    %(restaurant_name)s,
                    %(load_src)s
                )
            """,
            {
                "order_id": order.id,
                "order_date": order.date,
                "order_cost": order.cost,
                "order_payment": order.payment,
                "order_status": order.status,
                "user_id": order.user.id,
                "user_name": order.user.name,
                "user_login": order.user.login,
                "restaurant_id": order.restaurant.id,
                "restaurant_name": order.restaurant.name,
                "load_src": self._load_src
            }
        )

    def __get_items(self, order: Order) -> List[Dict]:
        items = []
        for product in order.products:
            items.append({
                "order_id": order.id,
                "restaurant_id": order.restaurant.id,
                "product_id": product.id,
                "product_name": product.name,
                "product_category": product.category,
                "load_src": self._load_src
            })

        return items

    def __load_items_temp_table(self, cur: Cursor, items_temp_table: str, order: Order) -> None:
        cur.executemany(
            f"""
                INSERT INTO {items_temp_table} (order_id, restaurant_id, product_id, product_name, product_category, 
                    load_src)
                VALUES 
                (
                    %(order_id)s,
                    %(restaurant_id)s,
                    %(product_id)s,
                    %(product_name)s,
                    %(product_category)s,
                    %(load_src)s
                )
            """,
            self.__get_items(order)
        )

    def __load_hubs(self, cur: Cursor, order_temp_table: str, items_temp_table: str) -> None:
        cur.execute(
            f"""
                INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                SELECT gen_random_uuid() AS h_order_pk, 
                       ott.order_id      AS order_id, 
                       ott.order_date      AS order_dt, 
                       NOW()             AS load_dt, 
                       ott.load_src      AS load_src
                FROM {order_temp_table} AS ott
                ON CONFLICT(order_id) DO NOTHING;

                INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                SELECT gen_random_uuid() AS h_user_pk, 
                       ott.user_id       AS user_id, 
                       NOW()             AS load_dt, 
                       ott.load_src      AS load_src
                FROM {order_temp_table} AS ott
                ON CONFLICT(user_id) DO NOTHING;

                INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                SELECT gen_random_uuid() AS h_restaurant_pk, 
                       ott.restaurant_id AS restaurant_id, 
                       NOW()             AS load_dt, 
                       ott.load_src      AS load_src
                FROM {order_temp_table} AS ott
                ON CONFLICT(restaurant_id) DO NOTHING;

                INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                SELECT gen_random_uuid() AS h_product_pk, 
                       itt.product_id    AS product_id, 
                       NOW()             AS load_dt, 
                       itt.load_src      AS load_src
                FROM {items_temp_table} AS itt
                ON CONFLICT(product_id) DO NOTHING;

                INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                SELECT gen_random_uuid() AS h_product_pk, 
                       pc.category_name  AS product_id, 
                       NOW()             AS load_dt, 
                       pc.load_src      AS load_src
                FROM (SELECT DISTINCT product_category AS category_name, 
                                      load_src AS load_src 
                      FROM {items_temp_table}) AS pc
                ON CONFLICT(category_name) DO NOTHING;
            """
        )

    def __load_h_temp_tables(self, cur: Cursor, order_temp_table: str, items_temp_table: str, h_order_temp_table: str,
                             h_items_temp_table: str) -> None:
        cur.execute(
            f"""
                CREATE TEMP TABLE {h_order_temp_table}
                ON COMMIT DROP 
                AS 
                (
                    SELECT  ho.h_order_pk       AS h_order_pk,
                            ott.order_id        AS order_id, 
                            ott.order_date      AS order_date, 
                            ott.order_cost      AS order_cost, 
                            ott.order_payment   AS order_payment, 
                            ott.order_status    AS order_status,
                            hu.h_user_pk        AS h_user_pk,
                            ott.user_id         AS user_id, 
                            ott.user_name       AS user_name, 
                            ott.user_login      AS user_login, 
                            hr.h_restaurant_pk  AS h_restaurant_pk,
                            ott.restaurant_id   AS restaurant_id, 
                            ott.restaurant_name AS restaurant_name,
                            ott.load_src        AS load_src
                    FROM {order_temp_table} AS ott
                    LEFT JOIN dds.h_order AS ho
                        ON ott.order_id = ho.order_id
                    LEFT JOIN dds.h_user AS hu
                        ON ott.user_id = hu.user_id
                    LEFT JOIN dds.h_restaurant AS hr
                        ON ott.restaurant_id = hr.restaurant_id
                );

                CREATE TEMP TABLE {h_items_temp_table}
                ON COMMIT DROP
                AS 
                (
                    SELECT  ho.h_order_pk           AS h_order_pk,
                            itt.order_id            AS order_id,
                            hr.h_restaurant_pk      AS h_restaurant_pk, 
                            itt.restaurant_id       AS restaurant_id,
                            hp.h_product_pk         AS h_product_pk, 
                            itt.product_id          AS product_id, 
                            itt.product_name        AS product_name, 
                            hc.h_category_pk        AS h_category_pk,
                            itt.product_category    AS product_category,
                            itt.load_src            AS load_src
                    FROM {items_temp_table} AS itt
                    LEFT JOIN dds.h_order AS ho
                        ON itt.order_id = ho.order_id
                    LEFT JOIN dds.h_restaurant AS hr
                        ON itt.restaurant_id = hr.restaurant_id
                    LEFT JOIN dds.h_product AS hp
                        ON itt.product_id = hp.product_id
                    LEFT JOIN dds.h_category AS hc
                        ON itt.product_category = hc.category_name
                );
            """
        )

    def __load_links(self, cur: Cursor, h_order_temp_table: str, h_items_temp_table: str) -> None:
        cur.execute(
            f"""
                INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                SELECT  gen_random_uuid()   AS hk_order_user_pk,
                        hott.h_order_pk     AS h_order_pk,
                        hott.h_user_pk      AS h_user_pk,
                        NOW()               AS load_dt,
                        hott.load_src       AS load_src
                FROM {h_order_temp_table} AS hott
                    LEFT JOIN dds.l_order_user AS lou
                     ON hott.h_order_pk = lou.h_order_pk
                        AND hott.h_user_pk = lou.h_user_pk
                WHERE lou.hk_order_user_pk IS NULL;
                
                INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                SELECT  gen_random_uuid()   AS hk_order_product_pk,
                        hitt.h_order_pk     AS h_order_pk,
                        hitt.h_product_pk   AS h_product_pk,
                        NOW()               AS load_dt,
                        hitt.load_src       AS load_src
                FROM {h_items_temp_table} AS hitt
                    LEFT JOIN dds.l_order_product AS lop
                     ON hitt.h_order_pk = lop.h_order_pk
                        AND hitt.h_product_pk = lop.h_product_pk
                WHERE lop.hk_order_product_pk IS NULL;
                
                INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, 
                    load_src)
                SELECT gen_random_uuid()    AS hk_product_restaurant_pk,
                       hitt.h_product_pk    AS h_product_pk,
                       hitt.h_restaurant_pk AS h_restaurant_pk,
                       NOW()                AS load_dt,
                       hitt.load_src        AS load_src
                FROM {h_items_temp_table} AS hitt
                    LEFT JOIN dds.l_product_restaurant AS lpr
                     ON hitt.h_product_pk = lpr.h_product_pk
                        AND hitt.h_restaurant_pk = lpr.h_restaurant_pk 
                WHERE lpr.hk_product_restaurant_pk IS NULL;
                
                INSERT INTO dds.l_product_category (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, 
                    load_src)
                SELECT gen_random_uuid()    AS hk_product_category_pk,
                       hitt.h_product_pk    AS h_product_pk,
                       hitt.h_category_pk   AS h_category_pk,
                       NOW()                AS load_dt,
                       hitt.load_src        AS load_src
                FROM {h_items_temp_table} AS hitt
                    LEFT JOIN dds.l_product_category AS lpc
                     ON hitt.h_product_pk = lpc.h_product_pk
                        AND hitt.h_category_pk = lpc.h_category_pk 
                WHERE lpc.hk_product_category_pk IS NULL;
            """
        )

    def __load_satellites(self, cur: Cursor, h_order_temp_table: str, h_items_temp_table: str) -> None:
        cur.execute(
            f"""
                INSERT INTO dds.s_order_cost (hk_order_cost_pk, h_order_pk, cost, payment, load_dt, load_src)
                SELECT COALESCE(soc.hk_order_cost_pk, gen_random_uuid()) AS hk_order_cost_pk,
                       hott.h_order_pk      AS h_order_pk,
                       hott.order_cost      AS cost,
                       hott.order_payment   AS payment,
                       NOW()                AS load_dt,
                       hott.load_src        AS load_src
                FROM {h_order_temp_table} AS hott
                LEFT JOIN dds.s_order_cost AS soc
                     ON hott.h_order_pk = soc.h_order_pk
                WHERE soc.hk_order_cost_pk IS NULL 
                    OR hott.order_cost <> soc.cost
                    OR hott.order_payment <> soc.payment;
                     
                INSERT INTO dds.s_order_status (hk_order_status_pk, h_order_pk, status, load_dt, load_src)
                SELECT COALESCE(sos.hk_order_status_pk, gen_random_uuid()) AS hk_order_status_pk,
                       hott.h_order_pk AS h_order_pk,
                       hott.order_status    AS status,
                       NOW()         AS load_dt,
                       hott.load_src  AS load_src
                FROM {h_order_temp_table} AS hott
                LEFT JOIN dds.s_order_status AS sos
                     ON hott.h_order_pk = sos.h_order_pk
                WHERE sos.hk_order_status_pk IS NULL 
                    OR hott.order_status <> sos.status;
                     
                INSERT INTO dds.s_restaurant_names (hk_restaurant_names_pk, h_restaurant_pk, name, load_dt, load_src)
                SELECT COALESCE(srn.hk_restaurant_names_pk, gen_random_uuid()) AS hk_restaurant_names_pk,
                       hott.h_restaurant_pk AS h_restaurant_pk,
                       hott.restaurant_name AS name,
                       NOW()                AS load_dt,
                       hott.load_src        AS load_src
                FROM {h_order_temp_table} AS hott
                LEFT JOIN dds.s_restaurant_names AS srn
                     ON hott.h_restaurant_pk = srn.h_restaurant_pk
                WHERE srn.hk_restaurant_names_pk IS NULL 
                    OR hott.restaurant_name <> srn.name;
                    
                INSERT INTO dds.s_user_names (hk_user_names_pk, h_user_pk, username, userlogin, load_dt, load_src)
                SELECT COALESCE(sun.hk_user_names_pk, gen_random_uuid()) AS hk_user_names_pk,
                       hott.h_user_pk AS h_user_pk,
                       hott.user_name           AS username,
                       hott.user_login           AS userlogin,
                       NOW()              AS load_dt,
                       hott.load_src        AS load_src
                FROM {h_order_temp_table} AS hott
                LEFT JOIN dds.s_user_names AS sun
                     ON hott.h_user_pk = sun.h_user_pk
                WHERE sun.hk_user_names_pk IS NULL 
                    OR hott.user_name <> sun.username
                    OR hott.user_login <> sun.userlogin;
                    
                INSERT INTO dds.s_product_names (hk_product_names_pk, h_product_pk, name, load_dt, load_src)
                SELECT  COALESCE(spn.hk_product_names_pk, gen_random_uuid()) AS hk_product_names_pk,
                        hitt.h_product_pk   AS h_product_pk,
                        hitt.product_name   AS name,
                        NOW()               AS load_dt,
                        hitt.load_src       AS load_src
                FROM {h_items_temp_table} hitt
                LEFT JOIN dds.s_product_names as spn
                    ON hitt.h_product_pk = spn.h_product_pk
                WHERE spn.hk_product_names_pk IS NULL 
                    OR hitt.product_name <> spn.name;
            """
        )
