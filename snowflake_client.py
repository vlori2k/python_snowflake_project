import snowflake.connector
import pandas as pd

from snowflake_config import SnowflakeConfig


class SnowflakeClient:
    def __init__(self, config: SnowflakeConfig):

        self.config = config
        self.conn = None

    def connect(self) -> None:
        last_error = None

        # 👇 endret fra accounts → connection_accounts
        for account in self.config.connection_accounts:
            try:
                self.conn = snowflake.connector.connect(
                    user=self.config.user,
                    password=self.config.password,
                    account=account,
                    warehouse=self.config.warehouse,
                    database=self.config.database,
                    schema=self.config.schema,
                    role=self.config.role,
                )

                print(f"Connected to Snowflake with account: {account}")
                return

            except Exception as e:
                last_error = e
                print(f"Failed to connect with account '{account}': {e}")

        raise RuntimeError("Could not connect to Snowflake") from last_error

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def fetch_df(self, sql: str) -> pd.DataFrame:
        if not self.conn:
            raise RuntimeError("Not connected to Snowflake")
        return pd.read_sql(sql, self.conn)
