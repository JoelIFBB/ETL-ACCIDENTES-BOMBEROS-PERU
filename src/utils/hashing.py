# src/utils/hashing.py
import hashlib
import json

def calculate_df_hash(data: list[dict]) -> str:
    """
    Calcula un hash SHA256 del contenido de una lista de dicts.
    Ordena por NroParte para garantizar determinismo sin importar
    el orden en que la página devuelva los registros.
    """
    data_sorted = sorted(data, key=lambda x: x.get("NroParte", ""))
    content = json.dumps(data_sorted, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()