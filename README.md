# POC Fil Rouge v3 — Service de transformation sémantique

Pipeline d'interopérabilité sémantique entre un Mainframe COBOL (zBANK) et une plateforme cloud de scoring crédit. Les enregistrements Mainframe à format fixe sont transformés en documents JSON-LD enrichis avec l'ontologie FIBO (Financial Industry Business Ontology) via des mappings SSSOM standardisés.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Client (NiFi / curl / test_api.py)                     │
│                  │ HTTP POST                            │
│  ┌───────────────▼──────────────────────────────────┐  │
│  │  FastAPI  :8000                                  │  │
│  │                                                  │  │
│  │  /api/v1/transform        (1 enregistrement)    │  │
│  │  /api/v1/transform/batch  (lot)                 │  │
│  │  /health  /api/v1/stats                         │  │
│  │                                                  │  │
│  │  ┌────────────┐  ┌───────────┐  ┌────────────┐  │  │
│  │  │ copybook   │  │   sssom   │  │  transform │  │  │
│  │  │ .py        │  │   .py     │  │  .py       │  │  │
│  │  └────────────┘  └───────────┘  └────────────┘  │  │
│  │                        │                         │  │
│  │              ┌─────────▼────────┐                │  │
│  │              │   storage.py     │                │  │
│  └──────────────┴─────────┬────────┴────────────────┘  │
│                            │                            │
│            ┌───────────────▼───────────────┐           │
│            │  MinIO  :9000  (bucket S3)    │           │
│            │  Console web  :9001           │           │
│            └───────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘
```

Le fichier SSSOM/TSV et le fichier .dat Mainframe sont montés en volume — aucun redéploiement n'est nécessaire pour mettre à jour les mappings.

---

## Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (inclut Docker Compose)
- Python 3.10+ et `pip install requests` pour exécuter `test_api.py`

---

## Démarrage rapide

```bash
# 1. Se placer dans le dossier du projet
cd POC_v3

# 2. Construire les images et démarrer les services
docker-compose up --build

# 3. Vérifier que tout est prêt (dans un autre terminal)
curl http://localhost:8000/health
```

Le premier démarrage télécharge les images Docker (~500 Mo) et compile l'image FastAPI. Les démarrages suivants sont quasi-instantanés.

Pour arrêter proprement :

```bash
docker-compose down
```

Pour effacer également les données MinIO :

```bash
docker-compose down -v
```

---

## Accès aux interfaces

| Interface | URL | Identifiants |
|---|---|---|
| Documentation API interactive (Swagger) | http://localhost:8000/docs | — |
| Documentation API alternative (ReDoc) | http://localhost:8000/redoc | — |
| Console web MinIO | http://localhost:9001 | minioadmin / minioadmin |

---

## Endpoints

| Méthode | Route | Tag | Description |
|---|---|---|---|
| `GET` | `/health` | Monitoring | Santé du service |
| `POST` | `/api/v1/transform` | Transformation | Un enregistrement Mainframe → JSON-LD |
| `POST` | `/api/v1/transform/batch` | Transformation | Lot d'enregistrements (max 500) |
| `GET` | `/api/v1/stats` | Monitoring | Statistiques de couverture session |
| `POST` | `/api/v1/mappings/upload` | Mappings | Upload d'un .xlsx SSSOM → conversion + rechargement à chaud |
| `GET` | `/api/v1/mappings` | Mappings | Informations sur les mappings actifs |
| `POST` | `/api/v1/simulate` | Simulation | Génère des enregistrements Mainframe synthétiques (Copybook) |

---

### `POST /api/v1/simulate`

Génère des enregistrements Mainframe synthétiques au format COBOL Copybook. Reproduit la logique du notebook poc_v2.py (cellule 2.3) : données personnelles via Faker fr_FR, valeurs codées pondérées (catégorie client, situation professionnelle, statut compte…), montants en centimes.

**Corps de la requête :**

```json
{
  "count": 5,
  "seed": 42,
  "transform": true,
  "store": false
}
```

- `count` : nombre d'enregistrements à générer (1–50, défaut 1).
- `seed` : graine aléatoire pour des résultats reproductibles (optionnel).
- `transform` : si `true`, chaque enregistrement est aussi transformé en JSON-LD FIBO.
- `store` : si `true` et `transform=true`, le JSON-LD est sauvegardé dans MinIO.

**Réponse :**

```json
{
  "total": 5,
  "seed": 42,
  "transformed": true,
  "records": [
    {
      "index": 0,
      "raw_record": "3821049571...",
      "parsed": { "ACCNO": "3821049571", "BALANCE": "0000456789", "..." },
      "document": { "@context": { "..." }, "mappedData": { "..." }, "..." },
      "coverage_pct": 85.0,
      "mapped_fields": 17,
      "unmapped_fields": 3,
      "storage": null
    }
  ]
}
```

**Exemple curl :**

```bash
# Générer 3 enregistrements bruts (sans transformation)
curl -X POST http://localhost:8000/api/v1/simulate \
  -H "Content-Type: application/json" \
  -d '{"count": 3, "transform": false}'

# Générer 1 enregistrement et le transformer en JSON-LD
curl -X POST http://localhost:8000/api/v1/simulate \
  -H "Content-Type: application/json" \
  -d '{"count": 1, "seed": 42, "transform": true, "store": true}'
```

Cet endpoint est particulièrement utile pour tester le pipeline complet sans avoir besoin d'un fichier `.dat` Mainframe réel.

---

### `GET /health`

Vérifie que le service est opérationnel et que le fichier SSSOM est bien chargé.

```bash
curl http://localhost:8000/health
```

```json
{
  "status": "ok",
  "mappings_loaded": 42,
  "mapping_set_id": "https://filrouge.poc/mappings/mainframe-fibo-v1",
  "storage_backend": "minio",
  "records_processed_session": 0
}
```

---

### `POST /api/v1/transform`

Transforme un enregistrement Mainframe unique (chaîne de 102 caractères au format COBOL Copybook) en document JSON-LD sémantique.

**Corps de la requête :**

```json
{
  "raw_record": "54321098761234000000850...",
  "store": true
}
```

- `raw_record` : enregistrement Mainframe brut, exactement 102 caractères.
- `store` : si `true`, le document JSON-LD est sauvegardé dans MinIO (ou en local en cas d'indisponibilité).

**Réponse :**

```json
{
  "success": true,
  "account_id": "5432109876",
  "coverage_pct": 85.0,
  "mapped_fields": 17,
  "unmapped_fields": 3,
  "storage": {
    "backend": "minio",
    "key": "jsonld-output/account_5432109876.jsonld"
  },
  "document": {
    "@context": { "fibo-fbc": "https://spec.edmcouncil.org/fibo/ontology/FBC/...", "..." },
    "@type": "fibo-fbc:ProductsAndServices/ClientsAndAccounts/Account",
    "@id": "mainframe:account/5432109876",
    "mappedData": {
      "stat_cpte": {
        "@type": "https://spec.edmcouncil.org/.../ActiveAccount",
        "rdfs:label": "Active Account",
        "_originalCode": "01",
        "_matchType": "skos:exactMatch",
        "_confidence": 0.9
      },
      "..."
    },
    "_dataLineage": {
      "sourceSystem": "zBANK/Mainframe",
      "extractionDate": "2026-04-13T10:00:00Z",
      "mappingVersion": "...",
      "totalFields": 20,
      "mappedFields": 17,
      "unmappedFields": 3,
      "coveragePct": 85.0,
      "fieldDetails": [ "..." ]
    }
  }
}
```

**Exemple curl :**

```bash
curl -X POST http://localhost:8000/api/v1/transform \
  -H "Content-Type: application/json" \
  -d '{
    "raw_record": "5432109876123400000085000Maria Garcia         19780415020300000450000002500000P1000250000000100320045065D0000050020240112",
    "store": true
  }'
```

---

### `POST /api/v1/transform/batch`

Transforme un lot d'enregistrements Mainframe en une seule requête (maximum 500). Les erreurs de parsing sont isolées — elles n'interrompent pas le traitement des enregistrements valides.

**Corps de la requête :**

```json
{
  "records": [
    "54321098761234...",
    "98765432101234..."
  ],
  "store": true
}
```

**Réponse :**

```json
{
  "total": 2,
  "success_count": 2,
  "error_count": 0,
  "avg_coverage_pct": 85.0,
  "results": [
    {
      "index": 0,
      "success": true,
      "account_id": "5432109876",
      "coverage_pct": 85.0,
      "mapped_fields": 17,
      "unmapped_fields": 3,
      "storage": { "backend": "minio", "key": "jsonld-output/account_5432109876.jsonld" },
      "document": { "..." }
    }
  ]
}
```

---

### `GET /api/v1/stats`

Retourne les statistiques de couverture agrégées sur l'ensemble des enregistrements traités depuis le démarrage du service.

```bash
curl http://localhost:8000/api/v1/stats
```

```json
{
  "session_records_processed": 11,
  "session_total_mapped": 187,
  "session_total_unmapped": 33,
  "session_avg_confidence": 0.872,
  "session_avg_coverage_pct": 85.0,
  "mapping_set_id": "https://filrouge.poc/mappings/mainframe-fibo-v1",
  "storage_backend": "minio"
}
```

Un taux de couverture inférieur à 80 % indique que le fichier SSSOM doit être étendu pour couvrir davantage de champs Mainframe.

---

## Structure des fichiers

```
POC_v3/
│
├── api/                        Code source du service FastAPI
│   │
│   ├── core/                   Modules métier (pas de dépendance à FastAPI)
│   │   ├── copybook.py         Définition des 20 champs Copybook et parser
│   │   │                       → parse_copybook_record(line) : str → dict
│   │   │
│   │   ├── sssom.py            Chargement du fichier SSSOM/TSV
│   │   │                       → load_sssom(path) : dict {metadata, curie_map, mappings}
│   │   │                       → build_lookup(sssom) : dict {subject_id → mapping}
│   │   │
│   │   ├── transform.py        Transformation sémantique
│   │   │                       → transform_record(...) : dict → document JSON-LD
│   │   │                       → build_jsonld_context(curie_map) : dict @context
│   │   │                       → map_field_value(...) : recherche SSSOM en 2 niveaux
│   │   │
│   │   ├── simulator.py        Générateur de données Mainframe synthétiques
│   │   │                       → generate_record(seed) : génère 1 enregistrement (dict)
│   │   │                       → generate_batch(count, seed) : lot d'enregistrements
│   │   │                       → to_copybook(record) : dict → chaîne Copybook 132 car.
│   │   │                       Utilise Faker fr_FR pour les données personnelles,
│   │   │                       avec pools pondérés pour les codes métier.
│   │   │
│   │   └── storage.py          Client de stockage unifié (MinIO + repli local)
│   │                           → StorageClient.save(account_id, document)
│   │
│   ├── main.py                 Application FastAPI : routes, modèles Pydantic,
│   │                           chargement SSSOM au démarrage (lifespan)
│   │
│   ├── requirements.txt        Dépendances Python (fastapi, uvicorn, boto3, pydantic)
│   └── Dockerfile              Image Python 3.12 slim, build en deux étapes
│
├── data/
│   └── mainframe_to_fibo.sssom.tsv   Fichier de mapping sémantique (42 mappings)
│                                      Monté en lecture seule dans le container API.
│                                      Pour mettre à jour les mappings : remplacer ce
│                                      fichier et redémarrer l'API (docker-compose restart api)
│
├── docker-compose.yml          Orchestration des 3 services :
│                               api, minio, mc-init (création du bucket)
│
├── .env                        Variables d'environnement (endpoints MinIO,
│                               credentials, nom du bucket, chemins)
│
└── test_api.py                 Script de validation end-to-end :
                                health, transform, batch, stats, cas d'erreur
```

---

## Mettre à jour les mappings SSSOM

### Via l'API (méthode recommandée — sans redémarrage)

Les data stewards déposent directement leur fichier Excel via l'endpoint dédié. Le service convertit le fichier, écrase le TSV existant et recharge les mappings en mémoire instantanément.

```bash
curl -X POST http://localhost:8000/api/v1/mappings/upload \
  -F "file=@SSSOM_Mapping_Mainframe_FIBO_EN.xlsx"
```

```json
{
  "success": true,
  "message": "Fichier 'SSSOM_Mapping_Mainframe_FIBO_EN.xlsx' converti et mappings rechargés avec succès. 42 mappings actifs.",
  "mapping_set_id": "https://filrouge.poc/mappings/mainframe-fibo-v1",
  "mapping_date": "2026-04-13",
  "mapping_rows": 42,
  "curie_prefixes": 8,
  "preview": [
    { "subject_id": "mainframe:WS-ACCNO", "predicate_id": "skos:exactMatch", "object_id": "fibo-fbc:AccountIdentifier" }
  ]
}
```

Les enregistrements transformés **après** cet appel utilisent immédiatement les nouveaux mappings. Aucun redémarrage Docker n'est nécessaire.

### Via le dossier `data/` (méthode manuelle)

Pour les mises à jour hors ligne ou dans un pipeline CI :

1. Éditer le fichier Excel `SSSOM_Mapping_Mainframe_FIBO_EN.xlsx` (dans le dossier `POC/`).
2. Relancer la conversion : `python POC/convert_to_sssom.py`.
3. Copier le fichier produit dans `POC_v3/data/`.
4. Redémarrer uniquement le service API :

```bash
docker-compose restart api
```

---

## Variables d'environnement

Toutes les variables sont définies dans `.env`. Créer un fichier `.env.local` pour les surcharges sans modifier le fichier versionné.

| Variable | Valeur par défaut | Description |
|---|---|---|
| `SSSOM_FILE` | `/app/data/mainframe_to_fibo.sssom.tsv` | Chemin du fichier SSSOM dans le container |
| `LOCAL_OUTPUT_DIR` | `/app/output` | Répertoire de repli si MinIO est indisponible |
| `MINIO_ENDPOINT` | `http://minio:9000` | URL de l'API MinIO |
| `MINIO_ACCESS_KEY` | `minioadmin` | Clé d'accès MinIO |
| `MINIO_SECRET_KEY` | `minioadmin` | Clé secrète MinIO |
| `MINIO_BUCKET` | `jsonld-output` | Nom du bucket de sortie |

---

## Lancer les tests

```bash
# Installer requests si nécessaire
pip install requests

# Lancer la suite de tests (service doit être démarré)
python test_api.py
```

Sortie attendue :

```
╔══════════════════════════════════════════════╗
║  POC Fil Rouge v3 — Tests API FastAPI        ║
╚══════════════════════════════════════════════╝

────────────────────────────────────────────────────────────
  GET /health
────────────────────────────────────────────────────────────
  Status          : ok
  Mappings loaded : 42
  ...
  ✓ OK

✅  Tous les tests passent.
```

---

## Prochaine étape — Intégration NiFi

La v3 expose une API REST que n'importe quel iPaaS peut appeler. La prochaine étape est d'intégrer Apache NiFi comme orchestrateur :

- **GetFile** : lire le fichier .dat Mainframe déposé dans un répertoire surveillé.
- **SplitText** : découper le fichier en enregistrements individuels (1 ligne = 1 message).
- **InvokeHTTP** : appeler `POST /api/v1/transform` pour chaque enregistrement.
- **RouteOnAttribute** : router selon le taux de couverture (succès / alerte / erreur).
- **PutS3Object** : écrire directement dans MinIO via le connecteur S3 natif de NiFi.

NiFi sera ajouté comme service supplémentaire dans `docker-compose.yml`.
