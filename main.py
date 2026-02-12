from snowflake_config import SnowflakeConfig
from snowflake_client import SnowflakeClient

config = SnowflakeConfig()
client = SnowflakeClient(config)

client.connect()

df = client.fetch_df("""
    SELECT *
    FROM VLO_SNOWFLAKE_LAB.ANALYTICS.MART_CUSTOMER_KPIS
    LIMIT 10
""")

print(df)

client.close()
