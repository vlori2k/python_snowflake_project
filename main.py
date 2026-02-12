from __future__ import annotations  # Lar oss bruke type hints som "SnowflakeJobRunner" uten å bekymre oss for rekkefølge på imports

from pathlib import Path  # Brukes for å lage mapper (exports/) på en robust måte

from snowflake_config import SnowflakeConfig  # Leser Snowflake-config (user, password, account candidates osv.)
from snowflake_client import SnowflakeClient  # Client som kobler til Snowflake og holder connection
from snowflake_jobs import SnowflakeJobRunner  # "Job layer" som gir execute/copy-into/get/rowcount osv.
from etl_pipeline import ETLPipeline  # Enkel pipeline-orchestrator med steg (steps) og rapport


# Fullt kvalifisert tabellnavn til mart-tabellen din i Snowflake
MART_TABLE = "VLO_SNOWFLAKE_LAB.ANALYTICS.MART_CUSTOMER_KPIS"

# Navn på en intern Snowflake stage vi bruker til eksport
EXPORT_STAGE = "VLO_EXPORT_STAGE"

# Lokal mappe der eksportfiler lastes ned
EXPORT_DIR = "exports"


def main() -> None:
    # Lager config-objektet (typisk fra env/.env eller defaults)
    config = SnowflakeConfig()

    # Oppretter en pipeline med et navn (brukes mest for lesbarhet / logging / rapport)
    pipeline = ETLPipeline(name="customer_kpis_export_top10")

    # Lager klienten som skal koble til Snowflake
    client = SnowflakeClient(config)

    # Kobler til Snowflake (prøver flere account candidates om du har det satt opp sånn)
    client.connect_to_snowflake()

    # Lager job-runneren som bruker eksisterende connection for å gjøre Snowflake-operasjoner
    jobs = SnowflakeJobRunner(client)

    # ---- STEG 1: Session info ----
    # Vi legger til et steg i pipeline. Det er en funksjon (lambda) som returnerer en status-string.
    # session_info() kjører en liten SELECT for å hente konto, region, user, role.
    pipeline.add_steps_to_pipeline("session_info", lambda: f"Session: {client.show_session_info()}")

    # ---- STEG 2: Validering ----
    # Teller antall rader i mart-tabellen. Dette fungerer som enkel datakvalitetssjekk før export.
    pipeline.add_steps_to_pipeline(
        "validate_mart_has_rows",
        lambda: f"{MART_TABLE} rows={jobs.rowcount(MART_TABLE)}",
    )

    # ---- STEG 3: COPY INTO stage ----
    # Vi lager en egen funksjon fordi steget er flere linjer og har litt logikk.
    def copy_into_stage() -> str:
        # Query-en vi vil eksportere. Her tar vi top 10 customers.
        query = f"""
        SELECT *
        FROM {MART_TABLE}
        ORDER BY lifetime_value DESC
        LIMIT 10
        """

        # COPY INTO @stage/prefix/ FROM ( <query> )
        # Dette er en Snowflake-native eksport, og ser veldig "data engineering" ut.
        stage_path = jobs.copy_query_to_stage_csv(
            sql_query=query,                 # SQL-query som skal eksporteres
            stage_name=EXPORT_STAGE,         # Snowflake stage som filene skrives til
            stage_prefix="customer_kpis/top10",  # "folder path" inne i stage
            overwrite=True,                  # Overskriv tidligere eksport med samme path
            single=True,                     # Forsøk å få én fil (praktisk for demo)
        )

        # Returnerer en kort status-melding som blir lagret i pipeline rapporten
        return f"Copied to stage: {stage_path}"

    # Legger COPY INTO-steget inn i pipeline
    pipeline.add_steps_to_pipeline("copy_into_stage", copy_into_stage)

    # ---- STEG 4: GET fra stage til lokal mappe ----
    def download_from_stage() -> str:
        # Lager exports/-mappen lokalt hvis den ikke finnes
        Path(EXPORT_DIR).mkdir(parents=True, exist_ok=True)

        # GET @stage/path file://<local_path>/
        # Dette laster ned filene som COPY INTO la på stage.
        res = jobs.download_stage_to_local(
            "@VLO_EXPORT_STAGE/customer_kpis/top10/",  # Stage-path må matche det vi brukte i COPY INTO
            EXPORT_DIR,                                # Lokal mappe der filene lagres
        )

        # Returnerer statusmelding for rapport
        return f"Downloaded {res.files_downloaded} file(s) to: {res.local_dir}"

    # Legger download-steget inn i pipeline
    pipeline.add_steps_to_pipeline("download_from_stage", download_from_stage)

    # ---- Kjør pipeline ----
    # Pipeline kjører steg i rekkefølge, og stopper hvis et steg feiler (fail-fast).
    results = pipeline.run_pipeline()

    # Lukker Snowflake-connection etter pipeline-run
    client.close_connection()

    # ---- Print rapport ----
    # Skriver en enkel rapport som ser bra ut i terminal og i GitHub README-screenshots.
    print("\nPIPELINE REPORT")
    for r in results:
        status = "OK" if r.ok else "FAIL"
        print(f"- {status} {r.name}: {r.message}")


# Standard Python entrypoint
if __name__ == "__main__":
    main()
