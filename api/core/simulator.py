# -*- coding: utf-8 -*-
"""
Simulateur de données Mainframe.
Reproduit la logique de génération du notebook poc_v2.py (cellule 2.3).
Génère des enregistrements synthétiques au format COBOL Copybook à largeur fixe.
"""

import random
import datetime
from core.copybook import format_copybook_record

try:
    from faker import Faker
    _faker_available = True
except ImportError:
    _faker_available = False


# ── Pools de valeurs codées (distributions pondérées) ────────────────────────

CATG_CLT_POOL  = ["01", "01", "01", "02"]               # 75% particulier, 25% entreprise
SIT_PRO_POOL   = ["01", "02", "02", "03", "04", "05"]   # salarié majoritaire
STAT_CPTE_POOL = ["01", "01", "01", "01", "02", "03", "09"]  # majorité actifs
TYP_ENGAG_POOL = ["P1", "P1", "P2", "P3", "P4", "D1"]
INC_PAY_POOL   = ["00", "00", "00", "00", "00", "01", "01", "02", "03"]  # majorité clean
ACTION_POOL    = ["D", "D", "W", "W", "T"]


def generate_record(seed: int | None = None) -> dict:
    """
    Génère un enregistrement Mainframe synthétique sous forme de dictionnaire.
    Utilise Faker pour les données personnelles si disponible,
    sinon repli sur des valeurs générées aléatoirement.
    """
    if seed is not None:
        random.seed(seed)

    if _faker_available:
        fake = Faker("fr_FR")
        if seed is not None:
            Faker.seed(seed)
        full_name = fake.name()[:30]
        dob = fake.date_of_birth(minimum_age=22, maximum_age=75)
        tx_date = fake.date_between(start_date="-30d", end_date="today")
    else:
        # Repli sans Faker
        full_name = "Client Synthetique"[:30]
        year  = random.randint(1950, 2000)
        month = random.randint(1, 12)
        day   = random.randint(1, 28)
        dob   = datetime.date(year, month, day)
        tx_date = datetime.date.today() - datetime.timedelta(days=random.randint(0, 30))

    income  = random.randint(120000, 800000)   # 1 200 – 8 000 EUR en centimes
    charges = random.randint(30000, int(income * 0.7))
    balance = random.randint(-50000, 2000000)
    inc_pay = random.choice(INC_PAY_POOL)

    return {
        "ACCNO":         str(random.randint(1000000000, 9999999999)),
        "PIN":           str(random.randint(1000, 9999)),
        "BALANCE":       str(abs(balance)).zfill(10),
        "CLT_NOM":       full_name.ljust(30)[:30],
        "CLT_DOB":       dob.strftime("%Y%m%d"),
        "CATG_CLT":      random.choice(CATG_CLT_POOL),
        "SIT_PRO":       random.choice(SIT_PRO_POOL),
        "REVENU_MENS":   str(income).zfill(10),
        "CHARGES_MENS":  str(charges).zfill(10),
        "STAT_CPTE":     random.choice(STAT_CPTE_POOL),
        "TYP_ENGAG":     random.choice(TYP_ENGAG_POOL),
        "MONTANT_ENGAG": str(random.randint(100000, 50000000)).zfill(10),
        "INC_PAY":       inc_pay,
        "NB_INC_12M":    str(random.randint(0, 3) if inc_pay != "00" else 0).zfill(3),
        "FLAG_DECVRT":   "1" if balance < 0 else "0",
        "ANCIENNETE":    str(random.randint(3, 360)).zfill(4),
        "SCORE_INT":     str(max(100, min(999, 650 + random.randint(-200, 200)))).zfill(3),
        "ACTION":        random.choice(ACTION_POOL),
        "AMOUNT":        str(random.randint(1000, 500000)).zfill(10),
        "TX_DATE":       tx_date.strftime("%Y%m%d"),
    }


def generate_batch(count: int, seed: int | None = None) -> list[dict]:
    """
    Génère un lot de `count` enregistrements.
    Si seed est fourni, les enregistrements sont déterministes et reproductibles.
    """
    if seed is not None:
        random.seed(seed)
        if _faker_available:
            Faker.seed(seed)

    records = []
    for i in range(count):
        rec_seed = (seed + i) if seed is not None else None
        records.append(generate_record(seed=rec_seed))
    return records


def to_copybook(record: dict) -> str:
    """Convertit un dictionnaire en ligne Copybook à largeur fixe."""
    return format_copybook_record(record)
