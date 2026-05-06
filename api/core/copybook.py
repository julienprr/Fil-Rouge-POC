# -*- coding: utf-8 -*-
"""
Définition et parsing du format COBOL Copybook à largeur fixe.
Reproduit la structure du dataset VSAM zBANK étendu pour le scoring crédit.
"""

# (nom_champ, largeur, type)
COPYBOOK_FIELDS = [
    ("ACCNO",         10, "num"),  # Numéro de compte
    ("PIN",            4, "num"),  # Code PIN
    ("BALANCE",       10, "num"),  # Solde en centimes
    ("CLT_NOM",       30, "str"),  # Nom client
    ("CLT_DOB",        8, "num"),  # Date de naissance AAAAMMJJ
    ("CATG_CLT",       2, "num"),  # Catégorie client (01/02)
    ("SIT_PRO",        2, "num"),  # Situation professionnelle (01-05)
    ("REVENU_MENS",   10, "num"),  # Revenu mensuel en centimes
    ("CHARGES_MENS",  10, "num"),  # Charges mensuelles en centimes
    ("STAT_CPTE",      2, "num"),  # Statut compte (01/02/03/09)
    ("TYP_ENGAG",      2, "str"),  # Type engagement (P1/P2/P3/P4/D1)
    ("MONTANT_ENGAG", 10, "num"),  # Montant engagement en centimes
    ("INC_PAY",        2, "num"),  # Sévérité incident paiement (00-03)
    ("NB_INC_12M",     3, "num"),  # Nb incidents 12 mois
    ("FLAG_DECVRT",    1, "num"),  # Dépassement découvert (0/1)
    ("ANCIENNETE",     4, "num"),  # Ancienneté compte en mois
    ("SCORE_INT",      3, "num"),  # Score crédit interne (0-999)
    ("ACTION",         1, "str"),  # Dernière transaction (D/W/T)
    ("AMOUNT",        10, "num"),  # Montant dernière transaction
    ("TX_DATE",        8, "num"),  # Date transaction AAAAMMJJ
]

TOTAL_RECORD_LENGTH = sum(w for _, w, _ in COPYBOOK_FIELDS)


def parse_copybook_record(line: str) -> dict:
    """
    Découpe une ligne à largeur fixe selon COPYBOOK_FIELDS.
    Retourne un dictionnaire {nom_champ: valeur_brute}.
    Lève ValueError si la ligne est trop courte.
    """
    line = line.rstrip("\n")
    if len(line) < TOTAL_RECORD_LENGTH:
        raise ValueError(
            f"Enregistrement trop court : {len(line)} caractères "
            f"(attendu {TOTAL_RECORD_LENGTH})"
        )
    record = {}
    pos = 0
    for field_name, width, _ in COPYBOOK_FIELDS:
        record[field_name] = line[pos:pos + width].strip()
        pos += width
    return record


def format_copybook_record(values: dict) -> str:
    """
    Formate un dictionnaire en ligne Copybook à largeur fixe.
    Utilisé par le générateur de données de test.
    """
    line = ""
    for field_name, width, ftype in COPYBOOK_FIELDS:
        val = str(values.get(field_name, ""))
        if ftype == "num":
            line += val.rjust(width, "0")[:width]
        else:
            line += val.ljust(width)[:width]
    return line
