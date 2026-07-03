# 🚒 ETL-ACCIDENTES

**De la web de Bomberos Perú a tu base de datos analítica. Sin clicks, sin Excel, sin intervención manual.**

Un pipeline ETL que extrae emergencias en tiempo real del Cuerpo de Bomberos del Perú, las limpia aplicando Change Data Capture con histórico de cambios, y las modela en un Star Schema listo para dashboards de BI.

**1 comando para levantar todo:** `docker compose up -d`

![Python](https://img.shields.io/badge/Python-3.14-3776AB) ![Apache Airflow](https://img.shields.io/badge/Airflow-3.1.8-017CEE) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1) ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED) ![Great Expectations](https://img.shields.io/badge/Great_Expectations-passing-FF6F00) ![pytest](https://img.shields.io/badge/pytest-62_passing-0A9EDC)

---

## Resultados

- **9 tareas orquestadas** en Airflow, desde scraping hasta modelo analítico
- **7 tablas en PostgreSQL 16** con esquema dimensional listo para consultas BI
- **62 tests automatizados** con pytest (9/9 módulos cubiertos)
- **8 servicios Docker** (Airflow + Celery + PostgreSQL + Redis)
- **Desplegable con 1 comando**

## Diseño del pipeline

- **CDC tipo 2** en Silver con `es_actual` y `estado_anterior` — cada cambio de estado se registra sin perder el histórico
- **Hash SHA256** como guard rail — si los datos no cambiaron respecto a la ejecución anterior, se salta todo el procesamiento
- **Star Schema en Gold** con 4 dimensiones (Tiempo, Tipo, Distrito, Estado) + fact table con turno calculado — pensado para dashboards
- **Volumen compartido** entre workers de Celery — escalable horizontalmente sin cambiar una línea
- **Great Expectations** validando cada batch antes de persistir — la calidad de datos es parte del pipeline, no un paso aparte

## Arquitectura

![Arquitectura ETL](ETL-ACCIDENTES.png)

## Stack

| Categoría | Tecnología |
|---|---|
| Lenguaje | Python 3.14 con uv |
| Orquestación | Apache Airflow 3.1.8 + Celery + Redis |
| Base de datos | PostgreSQL 16 |
| Calidad de datos | Great Expectations |
| Infraestructura | Docker Compose (8 servicios) |
| Testing | pytest (62 tests, 9/9 módulos) |

## Cómo ejecutar

```bash
cp .env.example .env
docker compose up -d
```

Trigger manual del DAG `pipeline_accidents` en `http://localhost:8080`.

## Documentación clave

| Recurso | Descripción |
|---|---|
| [`scripts/initdb/`](scripts/initdb/) | Esquema dimensional completo (7 tablas: 4 dimensiones + 1 fact + índices) |
| [`docker-compose.yaml`](docker-compose.yaml) | Infraestructura: Airflow + Celery + PostgreSQL 16 + Redis |
| [`ETL-ACCIDENTES.png`](ETL-ACCIDENTES.png) | Diagrama de arquitectura del pipeline |

## Estructura

```
src/
├── extract/scraper.py          # Web scraping (fetch + parse HTML)
├── transform/                  # Transformaciones Silver → Gold
├── validation/                 # Great Expectations
├── load/                       # Carga CDC (Bronze + Silver + Gold + Hash)
├── utils/                      # db.py (conexión), hashing.py (SHA256)
dags/dag_accidents.py           # 9 tareas en secuencia
scripts/initdb/                 # 7 scripts SQL (esquema completo)
tests/
├── test_scraper.py             # Scraping
├── test_hashing.py             # SHA256
├── test_hash_to_storage.py     # Hash persistence
├── test_silver_to_storage.py   # CDC loader (Silver)
├── test_transform_silver.py    # Transformaciones Silver
├── test_transform_gold.py      # Star Schema (Gold)
├── test_validate_bronze.py     # Great Expectations
├── test_gold_to_storage.py     # Gold loader
└── test_raw_to_storage.py      # Bronze E/S
```
