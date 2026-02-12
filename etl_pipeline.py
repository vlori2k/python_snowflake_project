# etl_pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Tuple


@dataclass
class StepResult:
    name: str
    ok: bool
    message: str
    started_at: str
    finished_at: str


"""
ETLPipeline er en enkel "orchestrator" for ETL-jobber.

Hva den skal gjøre:
- Gi deg en strukturert måte å kjøre en ETL som en rekke tydelige steg (steps).
- Gjøre main.py ryddig ved å flytte kontrollflyt (hvilke steg som kjøres, i hvilken rekkefølge) inn i en klasse.
- Returnere en rapport (liste av StepResult) som kan printes eller logges, slik at jobben ser "prod-ready" ut.

Designvalg:
- Hvert steg er en funksjon som returnerer en kort status-melding (string).
- Hvis et steg feiler, stopper pipeline (fail-fast). Dette er ofte ønskelig i data-pipelines
  for å unngå at man eksporterer/transformerer på dårlig grunnlag.
- Tidsstempler (start/slutt) lagres per steg, så du kan dokumentere runtime og feilsøke enkelt.

Typisk bruk:
pipeline = ETLPipeline("daily_customer_export")
pipeline.add_step("connect", ...)
pipeline.add_step("validate_sources", ...)
pipeline.add_step("transform_marts", ...)
pipeline.add_step("export", ...)
results = pipeline.run_pipeline()
"""


class ETLPipeline:
    def __init__(self, name: str):
        self.name = name
        self.pipeline_steps: List[Tuple[str, Callable[[], str]]] = []  # Liste med pipeline-steg: (navn på steg, funksjon som kjøres og returnerer status-tekst)



    def add_steps_to_pipeline(self, name: str, fn: Callable[[], str]) -> None:
        self.pipeline_steps.append((name, fn))


    def run_pipeline(self) -> List[StepResult]:  # Returnerer en liste med StepResult (én per pipeline-steg)
        results: List[StepResult] = []

        for name, fn in self.pipeline_steps:
            started_at = datetime.utcnow().isoformat()

            try:
                message = fn()
                finished_at = datetime.utcnow().isoformat()

                results.append(StepResult(name=name, ok=True, message=message, started_at=started_at, finished_at=finished_at))

            except Exception as e:
                finished_at = datetime.utcnow().isoformat()

                # Logg feilen inn i pipeline-reporten
                results.append(StepResult(name=name, ok=False, message=str(e), started_at=started_at, finished_at=finished_at,))

                print("hva sier erroren?", e)

                raise


        return results
