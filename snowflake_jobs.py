# snowflake_jobs.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from snowflake_client import SnowflakeClient


@dataclass
class CopyOutResult:
    stage_path: str
    local_dir: str
    files_downloaded: int


"""
SnowflakeJobRunner er et lite "job layer" over SnowflakeClient som gjør koden mer ETL- og Snowflake-proff.

Hva den skal gjøre:
- Samle Snowflake-operasjoner som brukes i pipeline-jobber på ett sted, slik at main.py og andre scripts
  blir korte og lesbare.
- Gi enkle hjelpefunksjoner for å kjøre SQL (DDL/DML), hente én verdi fra en query, og gjøre valideringer
  (for eksempel rowcount på en tabell).
- Støtte Snowflake-native export (extract) ved å bruke COPY INTO til en stage, og deretter GET for å
  laste ned resultatfiler lokalt. Dette er mer "Snowflake-ekte" enn å hente alt til pandas og skrive CSV der.

Typisk bruk i en ETL-jobb:
1) Kjør CREATE/ALTER/INSERT/MERGE med execute()
2) Kjør valideringer med rowcount() / query_value()
3) Eksporter curated data fra en mart med COPY INTO @stage/path/
4) Hent eksportfilene ned til en lokal exports/-mappe med GET

Resultat:
- Du kan vise at du kan Snowflake patterns (stage, COPY INTO, GET), ETL-jobbsteg, og enkel datakvalitet,
  samtidig som koden er ryddig og gjenbrukbar.
"""


class SnowflakeJobRunner:
    def __init__(self, client: SnowflakeClient):
        self.client = client

    def execute(self, sql: str) -> None:
        if not self.client.snowflake_connection:
            raise RuntimeError("Not connected to Snowflake")
        cur = self.client.snowflake_connection.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close_connection()

    def query_value(self, sql: str):
        if not self.client.snowflake_connection:
            raise RuntimeError("Not connected to Snowflake")
        cur = self.client.snowflake_connection.cursor()
        try:
            cur.execute(sql)
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            cur.close_connection()

    def rowcount(self, fully_qualified_table: str) -> int:
        return int(self.query_value(f"SELECT COUNT(*) FROM {fully_qualified_table}"))

    def ensure_stage(self, stage_name: str) -> None:
        self.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")

    def copy_query_to_stage_csv(
        self,
        sql_query: str,
        stage_name: str,
        stage_prefix: str,
        overwrite: bool = True,
        single: bool = True,
    ) -> str:
        """
        COPY INTO @stage/prefix/ FROM ( <query> )
        Returnerer stage-path som ble brukt.
        """
        self.ensure_stage(stage_name)
        stage_path = f"@{stage_name}/{stage_prefix}".rstrip("/") + "/"

        overwrite_str = "TRUE" if overwrite else "FALSE"
        single_str = "TRUE" if single else "FALSE"

        copy_sql = f"""
        COPY INTO {stage_path}
        FROM (
            {sql_query.strip().rstrip(";")}
        )
        FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION=NONE)
        OVERWRITE = {overwrite_str}
        SINGLE = {single_str}
        HEADER = TRUE
        """
        self.execute(copy_sql)
        return stage_path


    def download_stage_to_local(self, stage_path: str, local_dir: str) -> CopyOutResult:
        """
        Bruker Snowflake GET for å hente filer lokalt.
        Krever at connector har tilgang til lokal filsti.
        """
        if not self.client.snowflake_connection:
            raise RuntimeError("Not connected to Snowflake")

        Path(local_dir).mkdir(parents=True, exist_ok=True)

        get_sql = f"GET {stage_path} file://{Path(local_dir).resolve().as_posix()}/"
        cur = self.client.snowflake_connection.cursor()
        try:
            cur.execute(get_sql)
            results = cur.fetchall() or []
            # GET returnerer typisk en rad per fil
            files_downloaded = len(results)
            return CopyOutResult(stage_path=stage_path, local_dir=local_dir, files_downloaded=files_downloaded)
        finally:
            cur.close_connection()


