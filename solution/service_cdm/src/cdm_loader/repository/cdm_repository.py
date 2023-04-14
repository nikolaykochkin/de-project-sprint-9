import uuid
from typing import Dict, List

from lib.pg import PgConnect
from psycopg import Cursor


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def save_message(self, message: List[Dict]):
        message_id = uuid.uuid4().hex
        temp_table = f"temp_user_stats_{message_id}"
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                self.__create_temp_table(cur, temp_table)
                self.__load_temp_table(cur, temp_table, message)
                self.__load_data_marts(cur, temp_table)

    def __create_temp_table(self, cur: Cursor, temp_table: str):
        cur.execute(
            f"""
                CREATE TEMP TABLE {temp_table}
                (
                    user_id         UUID,
                    product_id      UUID,
                    product_name    VARCHAR,
                    category_id     UUID,
                    category_name   VARCHAR,
                    order_cnt       INT
                )
                    ON COMMIT DROP;
            """
        )

    def __load_temp_table(self, cur: Cursor, temp_table: str, stats: List[Dict]):
        cur.executemany(
            f"""
                INSERT INTO {temp_table} (user_id, product_id, product_name, category_id, category_name, order_cnt)
                VALUES 
                (
                    %(user_id)s,
                    %(product_id)s,
                    %(product_name)s,
                    %(category_id)s,
                    %(category_name)s,
                    %(order_cnt)s
                )
            """,
            stats
        )

    def __load_data_marts(self, cur: Cursor, temp_table: str):
        cur.execute(
            f"""
                INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                SELECT  tt.user_id          AS user_id, 
                        tt.product_id       AS product_id, 
                        tt.product_name     AS product_name, 
                        SUM(tt.order_cnt)   AS order_cnt
                FROM {temp_table} AS tt
                GROUP BY user_id, product_id, product_name
                ON CONFLICT (user_id, product_id) DO UPDATE 
                SET product_name = EXCLUDED.product_name,
                    order_cnt = EXCLUDED.order_cnt;
                    
                INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt) 
                SELECT  tt.user_id          AS user_id, 
                        tt.category_id       AS category_id, 
                        tt.category_name     AS category_name, 
                        SUM(tt.order_cnt)   AS order_cnt
                FROM {temp_table} AS tt
                GROUP BY user_id, category_id, category_name
                ON CONFLICT (user_id, category_id) DO UPDATE 
                SET category_name = EXCLUDED.category_name,
                    order_cnt = EXCLUDED.order_cnt;
            """
        )
