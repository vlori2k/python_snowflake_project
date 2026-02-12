import snowflake.connector
import pandas as pd

from snowflake_config import SnowflakeConfig


class SnowflakeClient:
    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self.snowflake_connection = None

    """
    SnowflakeClient er en enkel klient-klasse som håndterer tilkobling mot Snowflake og gjør det lett å kjøre SQL.

    Hva klassen gjør:
    - Leser inn SnowflakeConfig og bruker den til å koble til Snowflake.
    - Prøver flere account-strenger (connection_accounts) helt til den finner en som fungerer.
    - Holder på én aktiv Snowflake-connection (snowflake_connection) som resten av koden kan bruke.
    - Gir hjelpefunksjoner for å:
      - hente data fra Snowflake som en pandas DataFrame (fetch_dataframe_from_sql)
      - hente session-info (show_session_info) for å verifisere konto, region, user og role
      - lukke connection ryddig (close_connection)

    Typisk bruk:
    1) client = SnowflakeClient(config)
    2) client.connect_to_snowflake()
    3) df = client.fetch_dataframe_from_sql("SELECT ...")
    4) info = client.show_session_info()
    5) client.close_connection()

    Resultat:
    - main.py og pipeline-kode blir ryddigere, fordi all connect/query/close-logikk ligger samlet i én klasse.
    """



    def connect_to_snowflake(self) -> None:
        last_error = None

        for account in self.config.connection_accounts:
            try:
                self.snowflake_connection = snowflake.connector.connect(
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



    def close_connection(self) -> None:
        if self.snowflake_connection:
            self.snowflake_connection.close()
            self.snowflake_connection = None



    def fetch_dataframe_from_sql(self, sql: str) -> pd.DataFrame:
        if not self.snowflake_connection:
            raise RuntimeError("Not connected to Snowflake")
        return pd.read_sql(sql, self.snowflake_connection)



    def show_session_info(self) -> dict:
        if not self.snowflake_connection:
            raise RuntimeError("Not connected to Snowflake")

        cur = self.snowflake_connection.cursor()
        try:
            cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_USER(), CURRENT_ROLE()")
            row = cur.fetchone()
            return {
                "current_account": row[0],
                "current_region": row[1],
                "current_user": row[2],
                "current_role": row[3],
            }
        finally:
            cur.close()
