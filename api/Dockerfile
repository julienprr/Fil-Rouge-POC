# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim

LABEL maintainer="POC Fil Rouge v3"
LABEL description="Service de transformation sémantique Mainframe → FIBO JSON-LD"

# Copier les dépendances installées
COPY --from=builder /install /usr/local

WORKDIR /app

# Copier le code applicatif
COPY main.py .
COPY core/ ./core/

# Répertoires montés par Docker Compose (données + sorties)
RUN mkdir -p /app/data /app/output

# Utilisateur non-root pour la sécurité
RUN useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

# Démarrage avec rechargement automatique en dev (--reload)
# En production, supprimer --reload et ajuster --workers
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
