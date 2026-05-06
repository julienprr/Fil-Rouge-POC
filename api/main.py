# -*- coding: utf-8 -*-
"""
POC Fil Rouge v3 — Service de transformation sémantique
FastAPI + SSSOM + FIBO JSON-LD

Endpoints :
  GET  /health                    Santé du service
  POST /api/v1/transform          Transformation d'un enregistrement unique
  POST /api/v1/transform/batch    Transformation d'un lot d'enregistrements
  GET  /api/v1/stats              Statistiques de couverture (session courante)
  POST /api/v1/mappings/upload    Upload d'un fichier Excel SSSOM → conversion + rechargement
  GET  /api/v1/mappings           Informations sur les mappings actifs
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Body, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from core.copybook import parse_copybook_record, TOTAL_RECORD_LENGTH
from core.sssom import load_sssom, build_lookup
from core.transform import transform_record, build_jsonld_context
from core.storage import StorageClient
from core.converter import convert_xlsx_to_sssom, ConversionError
from core.simulator import generate_batch, to_copybook

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("api")

# ── État global du service ───────────────────────────────────────────────────
state: dict = {}

SSSOM_PATH = os.getenv("SSSOM_FILE", "/app/data/mainframe_to_fibo.sssom.tsv")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Chargement du fichier SSSOM et initialisation du stockage au démarrage."""
    logger.info("Chargement du fichier SSSOM : %s", SSSOM_PATH)
    sssom = load_sssom(SSSOM_PATH)
    state["sssom"]          = sssom
    state["mapping_lookup"] = build_lookup(sssom)
    state["curie_map"]      = sssom["curie_map"]
    state["mapping_set_id"] = sssom["metadata"].get("mapping_set_id", "unknown")
    state["storage"]        = StorageClient()
    state["session_stats"]  = {"processed": 0, "total_mapped": 0, "total_unmapped": 0, "confidence_scores": []}
    logger.info(
        "Service prêt — %d mappings, backend stockage : %s",
        len(sssom["mappings"]), state["storage"].backend
    )
    yield
    logger.info("Arrêt du service.")


# ── Application ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="POC Fil Rouge v3 — Semantic Transformation API",
    description=(
        "Service de transformation sémantique : enregistrements Mainframe COBOL "
        "→ documents JSON-LD enrichis avec l'ontologie FIBO via mappings SSSOM."
    ),
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Modèles Pydantic ──────────────────────────────────────────────────────────
class TransformRequest(BaseModel):
    raw_record: Annotated[
        str,
        Field(
            description=f"Enregistrement Mainframe brut — chaîne de {TOTAL_RECORD_LENGTH} caractères au format COBOL Copybook.",
            min_length=TOTAL_RECORD_LENGTH,
            examples=["5432109876123400000085000Maria Garcia         19780415020300000450000002500000P1000250000000100320045065D0000050020240112"],
        )
    ]
    store: bool = Field(default=True, description="Sauvegarder le résultat dans MinIO / stockage local.")


class BatchTransformRequest(BaseModel):
    records: Annotated[
        list[str],
        Field(description="Liste d'enregistrements Mainframe bruts.", min_length=1, max_length=500)
    ]
    store: bool = Field(default=True, description="Sauvegarder chaque résultat.")


class HealthResponse(BaseModel):
    status: str
    mappings_loaded: int
    mapping_set_id: str
    storage_backend: str
    records_processed_session: int


class TransformResponse(BaseModel):
    success: bool
    account_id: str
    coverage_pct: float
    mapped_fields: int
    unmapped_fields: int
    storage: dict | None
    document: dict


class BatchTransformResponse(BaseModel):
    total: int
    success_count: int
    error_count: int
    avg_coverage_pct: float
    results: list[dict]


class StatsResponse(BaseModel):
    session_records_processed: int
    session_total_mapped: int
    session_total_unmapped: int
    session_avg_confidence: float
    session_avg_coverage_pct: float
    mapping_set_id: str
    storage_backend: str


class MappingUploadResponse(BaseModel):
    success: bool
    message: str
    mapping_set_id: str
    mapping_date: str
    metadata_entries: int
    curie_prefixes: int
    mapping_rows: int
    preview: list[dict]


class MappingsInfoResponse(BaseModel):
    mapping_set_id: str
    mapping_date: str
    total_mappings: int
    curie_prefixes: list[str]
    sssom_file: str
    last_uploaded_by: str


class SimulateRequest(BaseModel):
    count: Annotated[
        int,
        Field(default=1, ge=1, le=50, description="Nombre d'enregistrements à générer (1–50).")
    ] = 1
    seed: int | None = Field(default=None, description="Graine aléatoire pour des résultats reproductibles.")
    transform: bool = Field(default=False, description="Si true, chaque enregistrement est également transformé en JSON-LD.")
    store: bool = Field(default=False, description="Si true et transform=true, le document JSON-LD est sauvegardé dans MinIO.")


class SimulateRecordItem(BaseModel):
    index: int
    raw_record: str
    parsed: dict
    document: dict | None = None
    coverage_pct: float | None = None
    mapped_fields: int | None = None
    unmapped_fields: int | None = None
    storage: dict | None = None


class SimulateResponse(BaseModel):
    total: int
    seed: int | None
    transformed: bool
    records: list[SimulateRecordItem]


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
def health():
    """Vérification de santé du service."""
    return HealthResponse(
        status="ok",
        mappings_loaded=len(state["sssom"]["mappings"]),
        mapping_set_id=state["mapping_set_id"],
        storage_backend=state["storage"].backend,
        records_processed_session=state["session_stats"]["processed"],
    )


@app.post("/api/v1/transform", response_model=TransformResponse, tags=["Transformation"])
def transform_single(req: TransformRequest):
    """
    Transforme un enregistrement Mainframe unique en document JSON-LD sémantique.
    """
    # Parsing Copybook
    try:
        record = parse_copybook_record(req.raw_record)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    # Transformation sémantique
    doc = transform_record(
        record,
        state["mapping_lookup"],
        state["curie_map"],
        state["mapping_set_id"],
    )

    lineage  = doc["_dataLineage"]
    account_id = record.get("ACCNO", "unknown")

    # Mise à jour des stats de session
    stats = state["session_stats"]
    stats["processed"]      += 1
    stats["total_mapped"]   += lineage["mappedFields"]
    stats["total_unmapped"] += lineage["unmappedFields"]
    for f in lineage["fieldDetails"]:
        stats["confidence_scores"].append(f["confidence"])

    # Stockage
    storage_info = None
    if req.store:
        storage_info = state["storage"].save(account_id, doc)

    return TransformResponse(
        success=True,
        account_id=account_id,
        coverage_pct=lineage["coveragePct"],
        mapped_fields=lineage["mappedFields"],
        unmapped_fields=lineage["unmappedFields"],
        storage=storage_info,
        document=doc,
    )


@app.post("/api/v1/transform/batch", response_model=BatchTransformResponse, tags=["Transformation"])
def transform_batch(req: BatchTransformRequest):
    """
    Transforme un lot d'enregistrements Mainframe.
    Les erreurs de parsing sont isolées — elles n'interrompent pas le traitement des autres.
    """
    results      = []
    success_count = 0
    error_count   = 0
    coverages     = []

    for i, raw in enumerate(req.records):
        try:
            record = parse_copybook_record(raw)
        except ValueError as e:
            error_count += 1
            results.append({"index": i, "success": False, "error": str(e)})
            continue

        doc = transform_record(
            record,
            state["mapping_lookup"],
            state["curie_map"],
            state["mapping_set_id"],
        )
        lineage    = doc["_dataLineage"]
        account_id = record.get("ACCNO", f"record_{i}")
        coverage   = lineage["coveragePct"]
        coverages.append(coverage)

        # Stats de session
        stats = state["session_stats"]
        stats["processed"]      += 1
        stats["total_mapped"]   += lineage["mappedFields"]
        stats["total_unmapped"] += lineage["unmappedFields"]
        for f in lineage["fieldDetails"]:
            stats["confidence_scores"].append(f["confidence"])

        # Stockage
        storage_info = None
        if req.store:
            storage_info = state["storage"].save(account_id, doc)

        success_count += 1
        results.append({
            "index":          i,
            "success":        True,
            "account_id":     account_id,
            "coverage_pct":   coverage,
            "mapped_fields":  lineage["mappedFields"],
            "unmapped_fields": lineage["unmappedFields"],
            "storage":        storage_info,
            "document":       doc,
        })

    avg_coverage = round(sum(coverages) / len(coverages), 1) if coverages else 0.0

    return BatchTransformResponse(
        total=len(req.records),
        success_count=success_count,
        error_count=error_count,
        avg_coverage_pct=avg_coverage,
        results=results,
    )


@app.get("/api/v1/stats", response_model=StatsResponse, tags=["Monitoring"])
def get_stats():
    """Statistiques de couverture agrégées sur la session courante."""
    stats = state["session_stats"]
    processed      = stats["processed"]
    total_fields   = stats["total_mapped"] + stats["total_unmapped"]
    scores         = stats["confidence_scores"]

    avg_coverage = (
        round(stats["total_mapped"] / total_fields * 100, 1)
        if total_fields > 0 else 0.0
    )
    avg_confidence = round(sum(scores) / len(scores), 3) if scores else 0.0

    return StatsResponse(
        session_records_processed=processed,
        session_total_mapped=stats["total_mapped"],
        session_total_unmapped=stats["total_unmapped"],
        session_avg_confidence=avg_confidence,
        session_avg_coverage_pct=avg_coverage,
        mapping_set_id=state["mapping_set_id"],
        storage_backend=state["storage"].backend,
    )


@app.post(
    "/api/v1/mappings/upload",
    response_model=MappingUploadResponse,
    tags=["Mappings"],
)
async def upload_mappings(
    file: UploadFile = File(
        ...,
        description="Fichier Excel SSSOM (.xlsx) déposé par les data stewards. "
                    "Doit contenir les feuilles 'SSSOM_Metadata' et 'Mappings'.",
    )
):
    """
    Dépose un nouveau fichier Excel de mapping SSSOM.

    Le service :
    1. Valide que le fichier est bien un .xlsx avec la structure attendue.
    2. Convertit le fichier en SSSOM/TSV et l'écrit dans /app/data/.
    3. Recharge les mappings en mémoire immédiatement, sans redémarrage.

    Les enregistrements transformés après cet appel utilisent les nouveaux mappings.
    """
    # Validation du type de fichier
    if not file.filename or not file.filename.endswith(".xlsx"):
        raise HTTPException(
            status_code=400,
            detail="Le fichier doit être au format .xlsx. "
                   f"Reçu : '{file.filename}'.",
        )

    xlsx_bytes = await file.read()
    if len(xlsx_bytes) == 0:
        raise HTTPException(status_code=400, detail="Le fichier est vide.")

    # Conversion Excel → SSSOM/TSV
    output_path = os.path.dirname(SSSOM_PATH) + "/mainframe_to_fibo.sssom.tsv"
    try:
        summary = convert_xlsx_to_sssom(xlsx_bytes, output_path)
    except ConversionError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception("Erreur inattendue lors de la conversion SSSOM")
        raise HTTPException(status_code=500, detail=f"Erreur de conversion : {e}")

    # Rechargement des mappings en mémoire (hot-reload)
    try:
        new_sssom = load_sssom(output_path)
        state["sssom"]          = new_sssom
        state["mapping_lookup"] = build_lookup(new_sssom)
        state["curie_map"]      = new_sssom["curie_map"]
        state["mapping_set_id"] = new_sssom["metadata"].get("mapping_set_id", "unknown")
        state["mapping_meta"]   = {
            "mapping_date":    new_sssom["metadata"].get("mapping_date", ""),
            "uploaded_file":   file.filename,
        }
    except Exception as e:
        logger.exception("Erreur lors du rechargement des mappings")
        raise HTTPException(
            status_code=500,
            detail=f"Conversion réussie mais rechargement échoué : {e}",
        )

    logger.info(
        "Mappings rechargés depuis '%s' — %d mappings actifs.",
        file.filename, summary["mapping_rows"],
    )

    return MappingUploadResponse(
        success=True,
        message=(
            f"Fichier '{file.filename}' converti et mappings rechargés avec succès. "
            f"{summary['mapping_rows']} mappings actifs."
        ),
        mapping_set_id=summary["mapping_set_id"],
        mapping_date=summary["mapping_date"],
        metadata_entries=summary["metadata_entries"],
        curie_prefixes=summary["curie_prefixes"],
        mapping_rows=summary["mapping_rows"],
        preview=summary["preview"],
    )


@app.get("/api/v1/mappings", response_model=MappingsInfoResponse, tags=["Mappings"])
def get_mappings_info():
    """
    Retourne les informations sur le jeu de mappings actuellement actif :
    identifiant, date, nombre de mappings, préfixes CURIE disponibles.
    """
    meta = state.get("mapping_meta", {})
    return MappingsInfoResponse(
        mapping_set_id=state["mapping_set_id"],
        mapping_date=meta.get("mapping_date", state["sssom"]["metadata"].get("mapping_date", "")),
        total_mappings=len(state["sssom"]["mappings"]),
        curie_prefixes=list(state["curie_map"].keys()),
        sssom_file=SSSOM_PATH,
        last_uploaded_by=meta.get("uploaded_file", "fichier initial (démarrage)"),
    )


@app.post("/api/v1/simulate", response_model=SimulateResponse, tags=["Simulation"])
def simulate(req: SimulateRequest):
    """
    Génère des enregistrements Mainframe synthétiques au format COBOL Copybook.

    Reproduit la logique de génération du notebook poc_v2.py (cellule 2.3) :
    données personnelles via Faker fr_FR, valeurs codées pondérées (CATG_CLT,
    SIT_PRO, STAT_CPTE, etc.), montants en centimes.

    - `count` : nombre d'enregistrements (1–50).
    - `seed`  : graine reproductible (optionnelle).
    - `transform` : si true, chaque enregistrement est transformé en JSON-LD FIBO.
    - `store` : si true et transform=true, le JSON-LD est sauvegardé dans MinIO.
    """
    raw_dicts = generate_batch(count=req.count, seed=req.seed)

    items: list[SimulateRecordItem] = []

    for i, rec_dict in enumerate(raw_dicts):
        raw_line = to_copybook(rec_dict)

        # Parsing Copybook pour exposer les champs structurés
        try:
            parsed = parse_copybook_record(raw_line)
        except ValueError:
            # Ne devrait pas arriver car le simulateur génère des données valides
            parsed = rec_dict

        item_kwargs: dict = {
            "index":      i,
            "raw_record": raw_line,
            "parsed":     parsed,
        }

        if req.transform:
            doc = transform_record(
                parsed,
                state["mapping_lookup"],
                state["curie_map"],
                state["mapping_set_id"],
            )
            lineage    = doc["_dataLineage"]
            account_id = parsed.get("ACCNO", f"sim_{i}")

            # Mise à jour des stats de session
            stats = state["session_stats"]
            stats["processed"]      += 1
            stats["total_mapped"]   += lineage["mappedFields"]
            stats["total_unmapped"] += lineage["unmappedFields"]
            for f in lineage["fieldDetails"]:
                stats["confidence_scores"].append(f["confidence"])

            storage_info = None
            if req.store:
                storage_info = state["storage"].save(account_id, doc)

            item_kwargs.update({
                "document":       doc,
                "coverage_pct":   lineage["coveragePct"],
                "mapped_fields":  lineage["mappedFields"],
                "unmapped_fields": lineage["unmappedFields"],
                "storage":        storage_info,
            })

        items.append(SimulateRecordItem(**item_kwargs))

    return SimulateResponse(
        total=req.count,
        seed=req.seed,
        transformed=req.transform,
        records=items,
    )
