# -*- coding: utf-8 -*-
"""
Couche de transformation sémantique.
Applique les mappings SSSOM pour produire des documents JSON-LD conformes FIBO.
"""

import datetime
import logging

logger = logging.getLogger(__name__)

# Correspondance champ Copybook → clé SSSOM
# tuple (clé_sssom, is_code_value) pour les champs à valeurs codées
# str pour les champs de données brutes
FIELD_MAPPING = {
    "ACCNO":         "WS-ACCNO",
    "PIN":           "WS-PIN",
    "BALANCE":       "WS-BALANCE",
    "CLT_NOM":       "CLT_NOM",
    "CLT_DOB":       "CLT_DOB",
    "CATG_CLT":      ("CATG_CLT",      True),
    "SIT_PRO":       ("SIT_PRO",       True),
    "REVENU_MENS":   "REVENU_MENS",
    "CHARGES_MENS":  "CHARGES_MENS",
    "STAT_CPTE":     ("STAT_CPTE",     True),
    "TYP_ENGAG":     ("TYP_ENGAG",     True),
    "MONTANT_ENGAG": "MONTANT_ENGAG",
    "INC_PAY":       ("INC_PAY",       True),
    "NB_INC_12M":    "NB_INC_12M",
    "FLAG_DECVRT":   "FLAG_DECVRT",
    "ANCIENNETE":    "ANCIENNETE",
    "SCORE_INT":     "SCORE_INT",
    "ACTION":        ("ACTION",        True),
    "AMOUNT":        "AMOUNT",
    "TX_DATE":       "TX_DATE",
}


def resolve_curie(curie: str, curie_map: dict) -> str:
    """Résout un CURIE (ex. fibo-loan:Loan) en URI complète."""
    if ":" not in curie:
        return curie
    prefix, local = curie.split(":", 1)
    base = curie_map.get(prefix, "")
    return base + local


def build_jsonld_context(curie_map: dict) -> dict:
    """Construit le bloc @context JSON-LD depuis le curie_map SSSOM."""
    context = {
        k: v for k, v in curie_map.items()
        if k not in ("semapv",)
    }
    context["xsd"]  = "http://www.w3.org/2001/XMLSchema#"
    context["rdfs"] = "http://www.w3.org/2000/01/rdf-schema#"
    return context


def map_field_value(
    field_name: str,
    raw_value: str,
    mapping_lookup: dict,
    curie_map: dict,
) -> tuple:
    """
    Recherche en deux niveaux :
      1. Code de valeur   : mainframe:{FIELD}_{VALUE}
      2. Champ de donnée  : mainframe:{FIELD}
    Retourne (fibo_uri, fibo_label, confidence, predicate_id) ou (None, None, 0, None).
    """
    # Niveau 1 — code de valeur
    code_key = f"mainframe:{field_name}_{raw_value}"
    if code_key in mapping_lookup:
        m = mapping_lookup[code_key]
        return (
            resolve_curie(m["object_id"], curie_map),
            m.get("object_label", ""),
            float(m.get("confidence", 0) or 0),
            m.get("predicate_id", ""),
        )

    # Niveau 2 — champ de donnée
    field_key = f"mainframe:{field_name}"
    if field_key in mapping_lookup:
        m = mapping_lookup[field_key]
        return (
            resolve_curie(m["object_id"], curie_map),
            m.get("object_label", ""),
            float(m.get("confidence", 0) or 0),
            m.get("predicate_id", ""),
        )

    return (None, None, 0, None)


def transform_record(
    record: dict,
    mapping_lookup: dict,
    curie_map: dict,
    mapping_set_id: str = "unknown",
) -> dict:
    """
    Transforme un enregistrement Mainframe parsé en document JSON-LD sémantique.
    """
    doc = {
        "@context": build_jsonld_context(curie_map),
        "@type":    "fibo-fbc:ProductsAndServices/ClientsAndAccounts/Account",
        "@id":      f"mainframe:account/{record.get('ACCNO', 'unknown')}",
    }

    mapped_fields   = {}
    unmapped_fields = {}
    lineage         = []

    for raw_field, sssom_key in FIELD_MAPPING.items():
        raw_value = record.get(raw_field, "")
        if not raw_value:
            continue

        if isinstance(sssom_key, tuple):
            sssom_field, is_code = sssom_key
            fibo_uri, fibo_label, conf, predicate = map_field_value(
                sssom_field, raw_value, mapping_lookup, curie_map
            )
        else:
            fibo_uri, fibo_label, conf, predicate = map_field_value(
                sssom_key, raw_value, mapping_lookup, curie_map
            )
            is_code = False

        prop_name = raw_field.lower()

        if fibo_uri:
            if is_code:
                mapped_fields[prop_name] = {
                    "@type":         fibo_uri,
                    "rdfs:label":    fibo_label,
                    "_originalCode": raw_value,
                    "_matchType":    predicate,
                    "_confidence":   conf,
                }
            else:
                mapped_fields[prop_name] = {
                    "@type":       fibo_uri,
                    "@value":      raw_value,
                    "_confidence": conf,
                }

            lineage.append({
                "sourceField":   raw_field,
                "sourceValue":   raw_value,
                "targetConcept": fibo_uri,
                "matchType":     predicate,
                "confidence":    conf,
            })
        else:
            unmapped_fields[raw_field] = raw_value

    doc["mappedData"] = mapped_fields
    if unmapped_fields:
        doc["unmappedData"] = unmapped_fields

    total = len(FIELD_MAPPING)
    mapped_count   = len(mapped_fields)
    unmapped_count = len(unmapped_fields)
    coverage = round(mapped_count / total * 100, 1) if total > 0 else 0

    doc["_dataLineage"] = {
        "sourceSystem":   "zBANK/Mainframe",
        "extractionDate": datetime.datetime.utcnow().isoformat() + "Z",
        "mappingVersion": mapping_set_id,
        "totalFields":    total,
        "mappedFields":   mapped_count,
        "unmappedFields": unmapped_count,
        "coveragePct":    coverage,
        "fieldDetails":   lineage,
    }

    return doc
