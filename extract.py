import logging
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def extract_cars():
    logger.info("INICIO DE EXTRACCIÓN DE AUTOS DE NEOAUTO")

    base_url = "https://neoauto.com/venta-de-autos-seminuevos"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    all_cars = []

    try:
        resp_init = requests.get(base_url, headers=headers, timeout=10)
        if resp_init.status_code != 200:
            return all_cars

        soup_init = BeautifulSoup(resp_init.content, "html.parser")
        last_page_link = soup_init.find("a", class_="c-pagination-content__last-page")
        if last_page_link and last_page_link.has_attr("href"):
            import re
            m = re.search(r"page=(\d+)", last_page_link["href"])
            total_paginas = int(m.group(1)) if m else 1
        else:
            total_paginas = 1
    except requests.RequestException:
        return all_cars

    for page in range(1, total_paginas + 1):
        url_page = f"{base_url}?page={page}"
        try:
            resp = requests.get(url_page, headers=headers, timeout=10)
            if resp.status_code != 200:
                break

            soup = BeautifulSoup(resp.content, "html.parser")
            listings = soup.select("article.c-results.c-results-used--premium")

            if not listings:
                break

            for card in listings:
                h2_tag = card.find("h2", class_="c-results__header-title")
                titulo = h2_tag.get_text(strip=True) if h2_tag else None

                precio_tag = card.find("div", class_="c-results-mount__price")
                precio_raw = precio_tag.get_text(strip=True) if precio_tag else None

                km_tag = card.find("span", class_="c-results-used__subtitle-description")
                if km_tag:
                    next_text = km_tag.find_next_sibling(text=True)
                    kilometraje = next_text.strip() if next_text else None
                else:
                    kilometraje = None

                ubic_tag = card.find("span", class_="c-results-details__description-text--highlighted")
                ubicacion = ubic_tag.get_text(strip=True) if ubic_tag else None

                descs = card.find_all("p", class_="c-results-details__description-text")
                combustible = None
                transmision = None
                if descs:
                    fuel_tag = descs[0].find("span", class_="c-results-used__detail-fuel")
                    combustible = fuel_tag.get_text(strip=True) if fuel_tag else None
                    texto_completo = descs[0].get_text(strip=True)
                    partes = [t.strip() for t in texto_completo.split("|")]
                    transmision = partes[-1] if len(partes) > 1 else None

                a_tag = card.find("a", class_="c-results__link", href=True)
                enlace = urljoin(base_url, a_tag["href"]) if a_tag else None

                all_cars.append({
                    "titulo":      titulo,
                    "precio_raw":  precio_raw,
                    "kilometraje": kilometraje,
                    "ubicacion":   ubicacion,
                    "combustible": combustible,
                    "transmision": transmision,
                    "enlace":      enlace
                })

            time.sleep(1)
        except requests.RequestException:
            break

    return all_cars
