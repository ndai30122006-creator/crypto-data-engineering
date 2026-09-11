"""Dagster resources (kết nối dùng chung)."""
import psycopg2
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    conn_str: str

    def get_conn(self):
        return psycopg2.connect(self.conn_str)
