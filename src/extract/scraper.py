import requests
import bs4 as bs
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# Configuramos el logger para que Airflow pueda capturar estos mensajes
logger = logging.getLogger(__name__)

def normalize_newlines(value: str) -> str:
    """Limpia saltos de línea y espacios en blanco de una cadena."""
    if value is None:
        return None
    return " | ".join(
        line.strip()
        for line in value.splitlines()
        if line.strip()
    )

def fetch_html(url: str) -> str:
    """Se encarga únicamente de la conexión y descarga del HTML."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BomberosPE-Scraper/1.0)"
    }
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)
    try:
        logger.info("Solicitando URL: %s", url)
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error("Error de conexión HTTP: %s", e)
        raise

def scrape_website(html_content: str) -> list[dict]:
    """Extrae datos de la tabla específica y los retorna como lista de dicts."""
    try:
        soup = bs.BeautifulSoup(html_content, 'lxml')
        
        table = soup.find('table', {'class': 'table table-sm table-bordered shadow'})

        if not table:
            raise ValueError("Tabla de emergencias no encontrada. Posible cambio en el HTML de la fuente.")

        all_data = []
        rows = table.find_all('tr')[1:]

        for row in rows:
            cells = row.find_all('td')
            if len(cells) == 7:
                all_data.append({
                    'NroParte': cells[0].text.strip(),
                    'Fecha_hora': cells[1].text.strip(),
                    'Direccion_distrito': cells[2].text.strip(),
                    'Tipo': cells[3].text.strip(),
                    'Estado': cells[4].text.strip(),
                    'Maquinas': normalize_newlines(cells[5].text.strip())
                })
            else:
                logger.debug("Fila omitida por formato inesperado (%d celdas).", len(cells))

        if not all_data:
            return []

        logger.info("Extracción exitosa: %d registros encontrados.", len(all_data))
        return all_data

    except Exception as e:
        logger.error("Error crítico en el parseo: %s", e)
        raise