# -*- coding: utf-8 -*-
"""
Moteur de scoring crédit à règles métier (règles simplifiées Bâle II/III).

Le scorer consomme le document JSON-LD FIBO produit par transform.py.
Il lit depuis `mappedData` (champs sémantiquement enrichis) et signale
explicitement les règles non applicables lorsqu'un champ est absent du
mapping SSSOM (présent dans `unmappedData`).

Cette dépendance directe à la couche sémantique est intentionnelle :
améliorer la couverture SSSOM améliore directement la qualité du scoring.

Références :
  - Bâle II (BCBS 128) — Pilier 1 : exigences minimales de fonds propres
  - Bâle III (BCBS 189) — ratio de levier, LCR
  - FIBO FND/AgL/Contracts, FIBO LOAN, FIBO FBC/ProductsAndServices
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

# ── Seuils de décision ────────────────────────────────────────────────────────

SEUIL_ACCORD = 65   # score >= 65 → accord
SEUIL_ALERTE = 45   # score 45–64 → revue manuelle requise
                    # score <  45 → refus

# ── Extraction depuis le JSON-LD ──────────────────────────────────────────────

def _get_mapped_value(doc: dict, field_name: str) -> str | None:
    """
    Extrait la valeur brute d'un champ depuis mappedData.
    - Champs codés  : _originalCode  (ex. stat_cpte → "01")
    - Champs données: @value         (ex. score_int → "650")
    Retourne None si le champ est absent de mappedData (non mappé).
    """
    field = doc.get("mappedData", {}).get(field_name)
    if field is None:
        return None
    # Champ codé (CATG_CLT, STAT_CPTE, INC_PAY…)
    if "_originalCode" in field:
        return field["_originalCode"]
    # Champ donnée (SCORE_INT, REVENU_MENS…)
    if "@value" in field:
        return field["@value"]
    return None


def _unmapped_rule(rule_id: str, fibo_concept: str, raw_field: str) -> tuple[float, str, str]:
    """Retourne une règle neutre (0 pt) avec un message explicite de non-couverture."""
    detail = (
        f"Champ '{raw_field}' absent de mappedData "
        f"(concept FIBO cible : {fibo_concept}) — règle non applicable, 0 pt"
    )
    return 0.0, rule_id, detail


# ── Règles individuelles ──────────────────────────────────────────────────────

def _rule_score_interne(doc: dict) -> tuple[float, str, str]:
    """
    Score interne normalisé (0 → 50 pts de base).
    FIBO : fibo-loan:CreditScore
    Source JSON-LD : mappedData.score_int[@value]
    """
    raw = _get_mapped_value(doc, "score_int")
    if raw is None:
        return _unmapped_rule("score_interne", "fibo-loan:CreditScore", "SCORE_INT")
    try:
        val = max(100, min(999, int(raw)))
    except ValueError:
        val = 650
    pts = round((val - 100) / (999 - 100) * 50, 2)
    return pts, "score_interne", f"SCORE_INT={val} → {pts}/50 pts de base"


def _rule_dti(doc: dict) -> tuple[float, str, str]:
    """
    Ratio dette/revenu — Debt-to-Income (DTI).
    FIBO : fibo-loan:DebtToIncomeRatio
    Source JSON-LD : mappedData.revenu_mens[@value] / mappedData.charges_mens[@value]
    Bâle II recommande DTI < 40 % pour les particuliers.
    """
    raw_rev = _get_mapped_value(doc, "revenu_mens")
    raw_chg = _get_mapped_value(doc, "charges_mens")

    if raw_rev is None or raw_chg is None:
        return _unmapped_rule("dti", "fibo-loan:DebtToIncomeRatio", "REVENU_MENS / CHARGES_MENS")

    try:
        revenu  = int(raw_rev) or 1
        charges = int(raw_chg)
    except ValueError:
        revenu, charges = 1, 0

    dti = charges / revenu

    if dti < 0.30:
        pts, note = +20, "DTI < 30 %"
    elif dti < 0.40:
        pts, note = +10, "DTI 30–40 %"
    elif dti < 0.50:
        pts, note =   0, "DTI 40–50 %"
    elif dti < 0.65:
        pts, note = -15, "DTI 50–65 %"
    else:
        pts, note = -30, "DTI > 65 %"

    return pts, "dti", f"{note} (DTI={dti:.1%}) → {pts:+d} pts"


def _rule_incidents_paiement(doc: dict) -> tuple[float, str, str]:
    """
    Incidents de paiement — INC_PAY.
    FIBO : fibo-loan:DelinquencyStatus
    Source JSON-LD : mappedData.inc_pay[_originalCode]
    """
    raw = _get_mapped_value(doc, "inc_pay")
    if raw is None:
        return _unmapped_rule("incidents_paiement", "fibo-loan:DelinquencyStatus", "INC_PAY")

    impacts = {"00": +15, "01": 0, "02": -15, "03": -30}
    labels  = {"00": "aucun", "01": "mineur", "02": "modéré", "03": "grave"}
    pts = impacts.get(raw, 0)
    return pts, "incidents_paiement", f"INC_PAY={raw} ({labels.get(raw, '?')}) → {pts:+d} pts"


def _rule_nb_incidents_12m(doc: dict) -> tuple[float, str, str]:
    """
    Nombre d'incidents sur 12 mois — NB_INC_12M.
    FIBO : fibo-loan:DelinquencyHistory
    Source JSON-LD : mappedData.nb_inc_12m[@value]
    """
    raw = _get_mapped_value(doc, "nb_inc_12m")
    if raw is None:
        return _unmapped_rule("nb_incidents_12m", "fibo-loan:DelinquencyHistory", "NB_INC_12M")

    try:
        nb = int(raw)
    except ValueError:
        nb = 0

    if nb == 0:
        pts = 0
    elif nb == 1:
        pts = -5
    elif nb == 2:
        pts = -10
    else:
        pts = -20

    return pts, "nb_incidents_12m", f"NB_INC_12M={nb} → {pts:+d} pts"


def _rule_statut_compte(doc: dict) -> tuple[float, str, str]:
    """
    Statut du compte — STAT_CPTE.
    FIBO : fibo-fbc:AccountStatus
    Source JSON-LD : mappedData.stat_cpte[_originalCode]
    """
    raw = _get_mapped_value(doc, "stat_cpte")
    if raw is None:
        return _unmapped_rule("statut_compte", "fibo-fbc:AccountStatus", "STAT_CPTE")

    impacts = {"01": +5, "02": -20, "03": -30, "09": -40}
    labels  = {"01": "actif", "02": "bloqué", "03": "clôturé", "09": "en incident"}
    pts = impacts.get(raw, 0)
    return pts, "statut_compte", f"STAT_CPTE={raw} ({labels.get(raw, '?')}) → {pts:+d} pts"


def _rule_decouvert(doc: dict) -> tuple[float, str, str]:
    """
    Indicateur de découvert — FLAG_DECVRT.
    FIBO : fibo-fbc:Overdraft
    Source JSON-LD : mappedData.flag_decvrt[@value]
    """
    raw = _get_mapped_value(doc, "flag_decvrt")
    if raw is None:
        return _unmapped_rule("decouvert", "fibo-fbc:Overdraft", "FLAG_DECVRT")

    pts = -10 if raw == "1" else 0
    return pts, "decouvert", f"FLAG_DECVRT={raw} → {pts:+d} pts"


def _rule_anciennete(doc: dict) -> tuple[float, str, str]:
    """
    Ancienneté du compte (en mois) — ANCIENNETE.
    FIBO : fibo-fbc:AccountOpeningDate (relation durée)
    Source JSON-LD : mappedData.anciennete[@value]
    """
    raw = _get_mapped_value(doc, "anciennete")
    if raw is None:
        return _unmapped_rule("anciennete", "fibo-fbc:AccountOpeningDate", "ANCIENNETE")

    try:
        mois = int(raw)
    except ValueError:
        mois = 0

    if mois < 12:
        pts, note = -5,  "< 12 mois"
    elif mois < 36:
        pts, note =  0,  "12–36 mois"
    elif mois < 120:
        pts, note = +5,  "36–120 mois"
    else:
        pts, note = +10, "> 120 mois"

    return pts, "anciennete", f"ANCIENNETE={mois} mois ({note}) → {pts:+d} pts"


# ── Moteur principal ──────────────────────────────────────────────────────────

_RULES = [
    _rule_score_interne,
    _rule_dti,
    _rule_incidents_paiement,
    _rule_nb_incidents_12m,
    _rule_statut_compte,
    _rule_decouvert,
    _rule_anciennete,
]


def score_from_jsonld(document: dict) -> dict:
    """
    Applique les règles métier sur un document JSON-LD FIBO.

    Lit exclusivement depuis `mappedData` — les champs absents du mapping
    SSSOM (dans `unmappedData`) rendent les règles correspondantes
    non applicables (0 pt, signalé explicitement dans les facteurs).

    Retourne :
      - score       : float [0–100]
      - decision    : "ACCORD" | "ALERTE" | "REFUS"
      - factors     : règles appliquées avec leur contribution
      - unmapped_rules : règles bloquées par un mapping manquant
    """
    factors       = []
    unmapped_rules = []
    total_pts     = 0.0

    for rule_fn in _RULES:
        pts, rule_id, detail = rule_fn(document)
        entry = {"rule": rule_id, "points": pts, "detail": detail}
        if "absent de mappedData" in detail:
            unmapped_rules.append(entry)
        else:
            factors.append(entry)
        total_pts += pts

    # Normalisation linéaire sur la plage théorique [-135, 100] → [0, 100]
    RAW_MIN, RAW_MAX = -135.0, 100.0
    score = (total_pts - RAW_MIN) / (RAW_MAX - RAW_MIN) * 100
    score = round(max(0.0, min(100.0, score)), 1)

    if score >= SEUIL_ACCORD:
        decision = "ACCORD"
    elif score >= SEUIL_ALERTE:
        decision = "ALERTE"
    else:
        decision = "REFUS"

    account_id = (
        document.get("@id", "unknown")
        .replace("mainframe:account/", "")
    )

    return {
        "account_id":    account_id,
        "score":         score,
        "decision":      decision,
        "factors":       factors,
        "unmapped_rules": unmapped_rules,
        "raw_points":    round(total_pts, 2),
        "thresholds":    {"accord": SEUIL_ACCORD, "alerte": SEUIL_ALERTE},
    }
