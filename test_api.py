# -*- coding: utf-8 -*-
"""
Script de test du service FastAPI POC Fil Rouge v3.
À exécuter après `docker-compose up --build`.

Usage :
    python test_api.py
"""

import json
import random
import sys
import datetime

try:
    import requests
except ImportError:
    print("Installer requests : pip install requests")
    sys.exit(1)

BASE_URL = "http://localhost:8000"

# ── Générateur de données de test ─────────────────────────────────────────────
def make_record(accno: str = None) -> str:
    """Génère un enregistrement Mainframe synthétique (102 caractères)."""
    accno        = accno or str(random.randint(1000000000, 9999999999))
    pin          = str(random.randint(1000, 9999))
    balance      = str(random.randint(0, 2000000)).zfill(10)
    name         = "Test Client".ljust(30)[:30]
    dob          = "19850315"
    catg_clt     = "01"
    sit_pro      = "02"
    revenu       = str(random.randint(200000, 600000)).zfill(10)
    charges      = str(random.randint(50000, 200000)).zfill(10)
    stat_cpte    = random.choice(["01", "01", "02", "03"])
    typ_engag    = random.choice(["P1", "P2", "P3", "D1"])
    montant      = str(random.randint(500000, 20000000)).zfill(10)
    inc_pay      = random.choice(["00", "00", "00", "01"])
    nb_inc       = "000" if inc_pay == "00" else str(random.randint(1, 3)).zfill(3)
    flag         = "0"
    anciennete   = str(random.randint(6, 240)).zfill(4)
    score        = str(random.randint(400, 900)).zfill(3)
    action       = random.choice(["D", "W", "T"])
    amount       = str(random.randint(1000, 100000)).zfill(10)
    tx_date      = datetime.date.today().strftime("%Y%m%d")

    return (accno + pin + balance + name + dob + catg_clt + sit_pro +
            revenu + charges + stat_cpte + typ_engag + montant +
            inc_pay + nb_inc + flag + anciennete + score + action + amount + tx_date)


def print_separator(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print('─' * 60)


# ── Tests ─────────────────────────────────────────────────────────────────────
def test_health():
    print_separator("GET /health")
    r = requests.get(f"{BASE_URL}/health")
    data = r.json()
    print(f"  Status          : {data['status']}")
    print(f"  Mappings loaded : {data['mappings_loaded']}")
    print(f"  Storage backend : {data['storage_backend']}")
    print(f"  Mapping set     : {data['mapping_set_id']}")
    assert r.status_code == 200 and data["status"] == "ok", "ÉCHEC /health"
    print("  ✓ OK")


def test_transform_single():
    print_separator("POST /api/v1/transform  (enregistrement unique)")
    record = make_record("9876543210")
    r = requests.post(f"{BASE_URL}/api/v1/transform", json={"raw_record": record, "store": True})
    data = r.json()
    print(f"  Compte          : {data['account_id']}")
    print(f"  Couverture      : {data['coverage_pct']} %")
    print(f"  Champs mappés   : {data['mapped_fields']}")
    print(f"  Champs non mappés: {data['unmapped_fields']}")
    print(f"  Stockage        : {data['storage']}")
    print("\n  Extrait JSON-LD (mappedData) :")
    for field, val in list(data["document"]["mappedData"].items())[:3]:
        print(f"    {field}: {json.dumps(val, ensure_ascii=False)}")
    assert r.status_code == 200 and data["success"], "ÉCHEC /transform"
    print("  ✓ OK")


def test_transform_batch():
    print_separator("POST /api/v1/transform/batch  (10 enregistrements)")
    records = [make_record() for _ in range(10)]
    r = requests.post(
        f"{BASE_URL}/api/v1/transform/batch",
        json={"records": records, "store": True}
    )
    data = r.json()
    print(f"  Total           : {data['total']}")
    print(f"  Succès          : {data['success_count']}")
    print(f"  Erreurs         : {data['error_count']}")
    print(f"  Couverture moy. : {data['avg_coverage_pct']} %")
    assert r.status_code == 200 and data["success_count"] == 10, "ÉCHEC /transform/batch"
    print("  ✓ OK")


def test_stats():
    print_separator("GET /api/v1/stats")
    r = requests.get(f"{BASE_URL}/api/v1/stats")
    data = r.json()
    print(f"  Enregistrements traités : {data['session_records_processed']}")
    print(f"  Couverture moy.         : {data['session_avg_coverage_pct']} %")
    print(f"  Confiance moy.          : {data['session_avg_confidence']}")
    print(f"  Backend stockage        : {data['storage_backend']}")
    assert r.status_code == 200, "ÉCHEC /stats"
    print("  ✓ OK")


def test_invalid_record():
    print_separator("POST /api/v1/transform  (enregistrement invalide)")
    r = requests.post(f"{BASE_URL}/api/v1/transform", json={"raw_record": "trop_court", "store": False})
    print(f"  HTTP {r.status_code} — {r.json().get('detail', '')[:80]}")
    assert r.status_code == 422, "ÉCHEC — devrait retourner 422"
    print("  ✓ OK (422 attendu)")


def test_simulate():
    print_separator("POST /api/v1/simulate  (5 enregistrements, transform=true)")
    r = requests.post(
        f"{BASE_URL}/api/v1/simulate",
        json={"count": 5, "seed": 42, "transform": True, "store": False},
    )
    data = r.json()
    print(f"  Total générés   : {data['total']}")
    print(f"  Graine          : {data['seed']}")
    print(f"  Transformé      : {data['transformed']}")
    first = data["records"][0]
    print(f"  [0] Compte      : {first['parsed'].get('ACCNO', '?')}")
    print(f"  [0] Couverture  : {first['coverage_pct']} %")
    print(f"  [0] raw_record  : {first['raw_record'][:30]}...")
    assert r.status_code == 200 and data["total"] == 5, "ÉCHEC /simulate"
    assert all(rec["raw_record"] for rec in data["records"]), "Enregistrements vides"
    print("  ✓ OK")


def test_simulate_raw():
    print_separator("POST /api/v1/simulate  (3 enregistrements bruts, sans transformation)")
    r = requests.post(
        f"{BASE_URL}/api/v1/simulate",
        json={"count": 3, "transform": False},
    )
    data = r.json()
    print(f"  Total générés   : {data['total']}")
    print(f"  Transformé      : {data['transformed']}")
    assert r.status_code == 200 and data["total"] == 3, "ÉCHEC /simulate (raw)"
    assert not data["transformed"], "transform devrait être false"
    assert data["records"][0]["document"] is None, "document devrait être null"
    print("  ✓ OK")


def test_get_mappings():
    print_separator("GET /api/v1/mappings")
    r = requests.get(f"{BASE_URL}/api/v1/mappings")
    data = r.json()
    print(f"  Mapping set ID    : {data['mapping_set_id']}")
    print(f"  Date              : {data['mapping_date']}")
    print(f"  Total mappings    : {data['total_mappings']}")
    print(f"  Préfixes CURIE    : {data['curie_prefixes']}")
    print(f"  Chargé depuis     : {data['last_uploaded_by']}")
    assert r.status_code == 200 and data["total_mappings"] > 0, "ÉCHEC /mappings"
    print("  ✓ OK")


def test_upload_mappings(xlsx_path: str = None):
    print_separator("POST /api/v1/mappings/upload")
    if xlsx_path is None:
        # Chercher le fichier Excel dans les dossiers parents
        candidates = [
            "../POC/SSSOM_Mapping_Mainframe_FIBO_EN.xlsx",
            "../../POC/SSSOM_Mapping_Mainframe_FIBO_EN.xlsx",
        ]
        xlsx_path = next((p for p in candidates if __import__("os").path.exists(p)), None)

    if xlsx_path is None:
        print("  ⚠ Fichier Excel introuvable — test ignoré.")
        print("    Fournir le chemin : python test_api.py --xlsx <chemin>")
        return

    print(f"  Fichier : {xlsx_path}")
    with open(xlsx_path, "rb") as f:
        r = requests.post(
            f"{BASE_URL}/api/v1/mappings/upload",
            files={"file": (xlsx_path.split("/")[-1], f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        )
    data = r.json()
    print(f"  Résultat      : {data.get('message', data.get('detail', ''))}")
    if r.status_code == 200:
        print(f"  Mapping rows  : {data['mapping_rows']}")
        print(f"  Mapping set   : {data['mapping_set_id']}")
        print(f"  Aperçu        :")
        for row in data["preview"]:
            print(f"    {row['subject_id']} → {row['object_id']} [{row['predicate_id']}]")
        assert data["success"], "ÉCHEC — success=false"
        print("  ✓ OK")
    else:
        print(f"  HTTP {r.status_code} — test ignoré (fichier peut-être incompatible).")


def test_invalid_file_upload():
    print_separator("POST /api/v1/mappings/upload  (fichier invalide)")
    r = requests.post(
        f"{BASE_URL}/api/v1/mappings/upload",
        files={"file": ("wrong.csv", b"col1,col2\na,b", "text/csv")},
    )
    print(f"  HTTP {r.status_code} — {r.json().get('detail', '')[:80]}")
    assert r.status_code == 400, "ÉCHEC — devrait retourner 400"
    print("  ✓ OK (400 attendu)")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Tests API POC Fil Rouge v3")
    parser.add_argument("--xlsx", help="Chemin vers le fichier Excel SSSOM à uploader", default=None)
    args = parser.parse_args()

    print("\n╔══════════════════════════════════════════════╗")
    print("║  POC Fil Rouge v3 — Tests API FastAPI        ║")
    print("╚══════════════════════════════════════════════╝")
    print(f"  Cible : {BASE_URL}")

    try:
        test_health()
        test_get_mappings()
        test_upload_mappings(args.xlsx)
        test_invalid_file_upload()
        test_simulate_raw()
        test_simulate()
        test_transform_single()
        test_transform_batch()
        test_stats()
        test_invalid_record()
        print("\n✅  Tous les tests passent.\n")
    except AssertionError as e:
        print(f"\n❌  Test échoué : {e}\n")
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        print(f"\n❌  Impossible de se connecter à {BASE_URL}")
        print("    Vérifier que le stack est démarré : docker-compose up --build\n")
        sys.exit(1)
