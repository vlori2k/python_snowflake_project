
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv
import os

# Last inn .env
load_dotenv()

@dataclass
class SnowflakeConfig:
    user: str = os.getenv("SNOWFLAKE_USER")
    password: str = os.getenv("SNOWFLAKE_PASSWORD")

    connection_accounts: List[str] = field(default_factory=lambda: [
        "lb52747.eu-north-1.aws",
        "LB52747.eu-north-1.aws",
        "lb52747",
        "LB52747",
    ])

    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE")
    database: str = os.getenv("SNOWFLAKE_DATABASE")
    schema: str = os.getenv("SNOWFLAKE_SCHEMA")
    role: str = os.getenv("SNOWFLAKE_ROLE")


