import logging
import re

logger = logging.getLogger(__name__)

def transform_cars(raw_list):
    logger.info("INICIO DE TRANSFORMACIÓN DE DATOS DE AUTOS")
    seen_links = set()
    transformed = []
    for entry in raw_list:
        titulo      = entry.get("titulo")
        precio_raw  = entry.get("precio_raw")
        kilometraje = entry.get("kilometraje")
        ubicacion   = entry.get("ubicacion")
        combustible = entry.get("combustible")
        transmision = entry.get("transmision")
        enlace      = entry.get("enlace")
        if not titulo or not precio_raw or not enlace:
            continue
        if not (enlace.startswith("http://") or enlace.startswith("https://")):
            continue
        if enlace in seen_links:
            continue
        seen_links.add(enlace)
        precio_digits = re.sub(r"[^\d\.]", "", precio_raw or "")
        try:
            precio_val = float(precio_digits)
        except (ValueError, TypeError):
            continue
        transformed.append({
            "titulo":      titulo,
            "precio":      precio_val,
            "kilometraje": kilometraje,
            "ubicacion":   ubicacion,
            "combustible": combustible,
            "transmision": transmision,
            "enlace":      enlace
        })
    return transformed