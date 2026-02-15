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


class SnowflakeJobRunner:  # Klasse som samler "job"-operasjoner mot Snowflake (SQL, export, osv.)
    def __init__(self, client: SnowflakeClient):  # Konstruktør som tar inn en SnowflakeClient
        self.snowflake_client = client  # Lagrer klienten så vi kan bruke connection inni metodene



    def execute_sql(self, sql: str) -> None:  # Kjører en SQL-kommando som ikke trenger å returnere data
        if not self.snowflake_client.snowflake_connection:  # Sjekker at vi har en aktiv connection
            raise RuntimeError("Not connected to Snowflake")  # Stopper tidlig hvis vi ikke er koblet til Snowflake
        cur = self.snowflake_client.snowflake_connection.cursor()  # Lager en cursor for å kjøre SQL
        try:
            cur.execute(sql) # kjører sql
        finally:
            cur.close()  # Lukker cursoren uansett om SQL-en feiler eller ikke



    def query_value(self, sql: str):  # Kjører en SQL og returnerer én enkelt verdi (første kolonne i første rad)
        if not self.snowflake_client.snowflake_connection:  # Sjekker at vi faktisk er koblet til Snowflake
            raise RuntimeError("Not connected to Snowflake")  # Kaster en feil hvis vi ikke har connection
        cur = self.snowflake_client.snowflake_connection.cursor()  # Lager en cursor som kan kjøre SQL mot Snowflake

        try:
            cur.execute(sql)  # NB: dette skal være cur.execute(sql), Snowflake-cursor har ikke execute_sql
            row = cur.fetchone()  # Henter første rad fra resultatet (eller None hvis ingen rader)
            return row[0] if row else None  # Returnerer første kolonne i raden, eller None hvis ingen rad
        finally:
            cur.close()  # Lukker cursoren uansett om query feiler eller ikke



    def count_rows(self, fully_qualified_table: str) -> int:  # Teller hvor mange rader det er i en gitt tabell
        return int(  # Konverterer resultatet til int

            self.query_value(  # Kjører en SQL som returnerer én verdi
                f"SELECT COUNT(*) FROM {fully_qualified_table}"  # SQL som teller rader i tabellen
            )
        )


    def create_stage_if_not_exists(self, stage_name: str) -> None:  # Sørger for at en Snowflake stage finnes (lager den hvis den ikke finnes)
        self.execute_sql(f"CREATE STAGE IF NOT EXISTS {stage_name}")  # Kjører SQL som oppretter stage trygt uten å feile hvis den allerede finnes



    def copy_query_results_to_stage_csv(  # Eksporterer resultatet av en SQL-query til en Snowflake stage som CSV
            self,  # Referanse til objektet (klassen du er inni)
            sql_query: str,  # SQL-en du vil eksportere (SELECT ...)
            stage_name: str,  # Navn på Snowflake stage (f.eks. VLO_EXPORT_STAGE)
            stage_prefix: str,  # "mappe/sti" inne i stage (f.eks. customer_kpis/top10)
            overwrite: bool = True,  # True = overskriv eksisterende filer på samme path
            single: bool = True,  # True = prøv å skrive resultatet til én fil
    ) -> str:  # Returnerer stage-pathen som ble brukt (@stage/prefix/)



        self.create_stage_if_not_exists(stage_name)  # Sørger for at stage finnes før vi eksporterer

        stage_path = f"@{stage_name}/{stage_prefix}".rstrip("/") + "/"  # Bygger stage-path og sikrer at den slutter med "/"

        overwrite_str = "TRUE" if overwrite else "FALSE"  # Gjør Python bool om til Snowflake TRUE/FALSE
        single_str = "TRUE" if single else "FALSE"  # Gjør Python bool om til Snowflake TRUE/FALSE



        clean_query = sql_query.strip().rstrip(";")  # Fjerner whitespace og semikolon så query passer trygt inni FROM (...)

        copy_sql = f"""
        COPY INTO {stage_path}
        FROM (
            {clean_query}
        )
        FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION=NONE)
        OVERWRITE = {overwrite_str}
        SINGLE = {single_str}
        HEADER = TRUE
        """

        self.execute_sql(copy_sql)
        return stage_path






    def download_stage_to_local(self, stage_path: str, local_dir: str) -> CopyOutResult:  # Laster ned filer fra Snowflake stage til en lokal mappe
        """

        Bruker Snowflake GET for å hente filer lokalt.
        Krever at connector har tilgang til lokal filsti.
        """


        if not self.snowflake_client.snowflake_connection:  # Sjekker at vi er koblet til Snowflake
            raise RuntimeError("Not connected to Snowflake")  # Stopper hvis vi ikke er koblet til Snowflake


        Path(local_dir).mkdir(parents=True, exist_ok=True)  # Lager lokal mappe hvis den ikke finnes

        get_sql = f"GET {stage_path} file://{Path(local_dir).resolve().as_posix()}/"  # Bygger GET-kommandoen (stage -> lokal filsti)

        cur = self.snowflake_client.snowflake_connection.cursor()  # Lager cursor for å kjøre GET i Snowflake

        try:
            cur.execute(get_sql)  # NB: skal være cur.execute(get_sql), ikke execute_sql
            results = cur.fetchall() or []  # Henter resultatet fra GET (ofte én rad per fil)
            files_downloaded = len(results)  # Teller hvor mange filer som ble lastet ned

            return CopyOutResult(stage_path=stage_path, local_dir=local_dir,
                                 files_downloaded=files_downloaded)  # Returnerer en oppsummering


        finally:  # Dette kjører ALLTID, enten try-blokka lykkes eller feiler med exception

            cur.close()  # Lukker cursoren så du ikke lekker ressurser (god praksis)
