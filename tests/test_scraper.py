# tests/test_scraper.py
import pytest
from src.extract.scraper import scrape_website, normalize_newlines


@pytest.fixture
def html_valido():
    """HTML que imita exactamente la estructura real de la página de bomberos."""
    return """
    <html><body>
        <table class="table table-sm table-bordered shadow">
            <tr>
                <th>#</th>
                <th>Nro Parte</th>
                <th>Fecha y hora</th>
                <th>Dirección / Distrito</th>
                <th>Tipo</th>
                <th>Estado</th>
                <th>Máquinas</th>
                <th>Ver Mapa</th>
            </tr>
            <tr>
                <td>2026022027</td>
                <td>27/06/2026 01:06:42</td>
                <td>MA. MIGUEL CHECA EGU</td>
                <td>ACCIDENTE VEHICULAR</td>
                <td>ATENDIENDO</td>
                <td>M2-1
AMB-176
M176-1</td>
                <td></td>
            </tr>
            <tr>
                <td>2026022028</td>
                <td>27/06/2026 02:00:00</td>
                <td>AV. AREQUIPA 123</td>
                <td>INCENDIO URBANO</td>
                <td>CONTROLADO</td>
                <td>Autobomba 01</td>
                <td></td>
            </tr>
        </table>
    </html></body>
    """


def test_extrae_registros_correctamente(html_valido):
    """HTML con 2 filas → devuelve exactamente 2 registros."""
    resultado = scrape_website(html_valido)
    assert len(resultado) == 2


def test_tabla_no_encontrada_lanza_error():
    """Si la tabla no existe en el HTML → lanza ValueError."""
    html_roto = "<html><body><p>Sin tabla</p></body></html>"
    with pytest.raises(ValueError, match="Tabla de emergencias no encontrada"):
        scrape_website(html_roto)


def test_filas_malformadas_se_omiten():
    """Filas con número incorrecto de celdas se omiten sin romper el pipeline."""
    html = """
    <html><body>
        <table class="table table-sm table-bordered shadow">
            <tr><th>#</th></tr>
            <tr><td>solo una celda</td></tr>
        </table>
    </html></body>
    """
    resultado = scrape_website(html)
    assert resultado == []


def test_maquinas_con_salto_de_linea_se_normaliza(html_valido):
    """Maquinas con saltos de línea queda separado por ' | '."""
    resultado = scrape_website(html_valido)
    assert resultado[0]["Maquinas"] == "M2-1 | AMB-176 | M176-1"