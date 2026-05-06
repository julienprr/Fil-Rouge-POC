# -*- coding: utf-8 -*-
"""
Module de stockage JSON-LD vers MinIO (compatible S3).
Si MinIO n'est pas disponible, repli automatique sur le stockage local.
"""

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Tentative d'import de boto3 (optionnel)
try:
    import boto3
    from botocore.exceptions import ClientError, EndpointConnectionError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    logger.warning("boto3 non disponible — stockage local uniquement.")


class StorageClient:
    """
    Client de stockage unifié.
    Tente MinIO en priorité, repli sur le système de fichiers local.
    """

    def __init__(self):
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.minio_access   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket         = os.getenv("MINIO_BUCKET", "jsonld-output")
        self.local_dir      = Path(os.getenv("LOCAL_OUTPUT_DIR", "/app/output"))
        self.local_dir.mkdir(parents=True, exist_ok=True)
        self._s3 = None
        self._minio_ok = False
        self._init_minio()

    def _init_minio(self):
        if not BOTO3_AVAILABLE:
            return
        try:
            self._s3 = boto3.client(
                "s3",
                endpoint_url=self.minio_endpoint,
                aws_access_key_id=self.minio_access,
                aws_secret_access_key=self.minio_secret,
            )
            # Vérifier/créer le bucket
            try:
                self._s3.head_bucket(Bucket=self.bucket)
            except ClientError:
                self._s3.create_bucket(Bucket=self.bucket)
                logger.info("Bucket MinIO créé : %s", self.bucket)
            self._minio_ok = True
            logger.info("Connecté à MinIO : %s / bucket : %s", self.minio_endpoint, self.bucket)
        except Exception as e:
            logger.warning("MinIO indisponible (%s) — repli sur stockage local.", e)
            self._minio_ok = False

    def save(self, account_id: str, document: dict) -> dict:
        """
        Sauvegarde un document JSON-LD.
        Retourne {"backend": "minio"|"local", "key": chemin_ou_clé}.
        """
        key = f"account_{account_id}.jsonld"
        content = json.dumps(document, ensure_ascii=False, indent=2)

        if self._minio_ok:
            try:
                self._s3.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=content.encode("utf-8"),
                    ContentType="application/ld+json",
                )
                logger.debug("MinIO ← %s", key)
                return {"backend": "minio", "key": f"{self.bucket}/{key}"}
            except Exception as e:
                logger.warning("Échec écriture MinIO (%s) — repli local.", e)

        # Repli local
        path = self.local_dir / key
        path.write_text(content, encoding="utf-8")
        logger.debug("Local ← %s", path)
        return {"backend": "local", "key": str(path)}

    @property
    def backend(self) -> str:
        return "minio" if self._minio_ok else "local"
