# -*- coding: utf-8 -*-
"""
Chargement et indexation du fichier SSSOM/TSV.
Construit le dictionnaire de lookup utilisé par la couche de transformation.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def load_sssom(filepath: str | Path) -> dict:
    """
    Parse un fichier .sssom.tsv et retourne un dictionnaire avec :
      - metadata   : paires clé/valeur de l'en-tête YAML
      - curie_map  : {préfixe: URI_de_base}
      - mappings   : liste de dicts {colonne: valeur}
    """
    metadata = {}
    curie_map = {}
    mappings = []
    header = None
    in_curie = False

    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Fichier SSSOM introuvable : {filepath}")

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")

            if line.startswith("#"):
                content = line[1:].strip()
                if not content:
                    in_curie = False
                    continue
                if content == "curie_map:":
                    in_curie = True
                    continue
                if in_curie and ":" in content:
                    prefix, uri = content.split(":", 1)
                    curie_map[prefix.strip()] = uri.strip().strip('"')
                elif ":" in content and not in_curie:
                    key, val = content.split(":", 1)
                    metadata[key.strip()] = val.strip().strip('"')
                continue

            if header is None:
                header = line.split("\t")
                continue

            values = line.split("\t")
            if len(values) < 2:
                continue
            row = {
                header[i]: values[i] if i < len(values) else ""
                for i in range(len(header))
            }
            mappings.append(row)

    logger.info(
        "SSSOM chargé : %d mappings, %d préfixes CURIE",
        len(mappings), len(curie_map)
    )
    return {"metadata": metadata, "curie_map": curie_map, "mappings": mappings}


def build_lookup(sssom: dict) -> dict:
    """
    Construit un dictionnaire {subject_id: mapping_row} pour lookup O(1).
    """
    return {m["subject_id"]: m for m in sssom["mappings"]}
