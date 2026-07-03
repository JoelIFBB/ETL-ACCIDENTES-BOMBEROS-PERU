# AGENTS.md — ETL-ACCIDENTES

Pipeline ETL que extrae emergencias en tiempo real del Cuerpo de Bomberos del Perú, aplica CDC con tracking histórico y modela los datos en un Star Schema sobre PostgreSQL 16.

**Stack**: Python 3.14 · Apache Airflow 3.1.8 · CeleryExecutor · PostgreSQL 16 · Redis · Docker Compose · Great Expectations · pytest

---

## 1. Arquitectura

```
🌐 SGNORTE → 🥉 Bronze (Raw JSON + GX + SHA256) → 🥈 Silver (CDC SCD2) → 🥇 Gold (Star Schema) → 🗄️ PostgreSQL 16
```

### Flujo del DAG (`pipeline_accidents` — 9 tareas secuenciales)

1. **task_fetch** — Descarga HTML con retry (3 intentos, backoff)
2. **task_parse** — Extrae tabla HTML → JSON (7 columnas)
3. **task_validate_bronze** — Great Expectations (row count ≥ 1, no nulos)
4. **task_hash_check** — SHA256 vs hash anterior → skip si no hay cambios
5. **task_upload** — Guarda raw JSON en `data/bronze/` particionado por fecha
6. **task_silver** — Limpieza → CDC upsert con histórico de cambios de estado
7. **task_gold** — Construye 4 dimensiones + 1 fact table con turno calculado
8. **task_save_hash** — Persiste hash en `pipeline_hash_log`
9. **task_cleanup** — Elimina archivos temporales (trigger_rule=all_done)

### Star Schema

| Tabla | Propósito |
|---|---|
| `DIM_TIPO` | Jerarquía de tipo de emergencia (categoría → subcategoría → detalle) |
| `DIM_DISTRITO` | Referencia de distritos de Lima |
| `DIM_ESTADO` | Estados posibles (ATENDIENDO, CERRADO, CONTROLADO, etc.) |
| `DIM_TIEMPO` | Dimensión temporal con PK YYYYMMDD, nombres en español |
| `FACT_EMERGENCIA` | Fact table con 4 FK, TURNO, coordenadas, conteo de máquinas |

---

## 2. Reglas NO negociables

| Regla | Explicación |
|---|---|
| **Logs y docstrings en español** | El dominio del negocio es Perú; el usuario final habla español |
| **Código (variables, funciones, clases) en inglés** | Estándar de la industria; cualquier ingeniero debe entender el código |
| **No usar GCS / Google Cloud** | Se eliminó todo rastro de GCP del proyecto |
| **El DAG es el pipeline** | No existe `main.py` ni scripts de ejecución directa |
| **No tocar `config/airflow.cfg`** | 3274 líneas de default de Airflow; solo 2 líneas custom (executor y sql_alchemy_conn) |
| **pytest solo en dev** | No incluir pytest en la imagen Docker |
| **Usar `uv`, no pip** | `uv sync` para instalar, `uv run` para ejecutar, `uv export` para requirements.txt |

---

## 3. Mapa de módulos

### `src/extract/scraper.py` — Web scraping

- `fetch_html(url, max_retries, delay)` — GET con retry + backoff
- `scrape_website(html)` — BeautifulSoup: extrae tabla de 7 columnas
- `normalize_newlines(value)` — Normaliza saltos de línea en columna Maquinas

### `src/transform/transform_silver.py` — Limpieza y transformación a Silver

- `transform_to_silver(records)` — Orquesta todas las transformaciones
- `clean_text_column(df, column)` — Elimina espacios sobrantes
- `column_todate(df, column)` — Convierte `Fecha_hora` a datetime
- `split_address(df, column, col_address, col_district)` — Separa dirección y distrito
- `extract_coordinates(df, column)` — Extrae latitud/longitud de coordenadas
- `split_tipo(df, column)` — Descompone `Tipo` en 3 niveles jerárquicos
- `count_machineries(df, column)` — Cuenta máquinas separadas por `|`

### `src/transform/transform_gold.py` — Modelo dimensional Gold

- `transform_to_gold(records)` — Orquesta dimensiones + fact
- `build_dim_tipo(df)` — DIM_TIPO con jerarquía
- `build_dim_distrito(df)` — DIM_DISTRITO
- `build_dim_estado(df)` — DIM_ESTADO
- `build_dim_tiempo(df)` — DIM_TIEMPO con PK YYYYMMDD
- `build_fact_emergencia(df)` — Fact table con llaves naturales + TURNO (FKs se resuelven en el loader)

### `src/validation/validate_bronze.py` — Data Quality

- `validate_bronze(records)` — Great Expectations: row count ≥ 1, no nulos en NroParte/Fecha_hora, columnas esperadas

### `src/load/raw_to_storage.py` — Carga Bronze

- `upload_raw_data(records, ingestion_datetime)` — Guarda JSON particionado en `data/bronze/accidentes/`

### `src/load/silver_to_storage.py` — Carga Silver con CDC

- `upload_silver_data(df)` — CDC: consulta estados actuales vs batch, decide INSERT/UPDATE/ignorar
- `_get_estados_actuales(cur, nro_partes)` — Query única con `ANY(%s)` para todo el batch
- `_insert_nuevo(cur, row)` — INSERT con `es_actual=TRUE`
- `_update_estado(cur, row, estado_anterior)` — Marca FALSE + INSERT nuevo con historial
- Retorna `{"insertados": N, "actualizados": N, "ignorados": N}`

### `src/load/gold_to_storage.py` — Carga Gold (Star Schema)

- `upload_gold_data(gold_tables)` — Upserts 4 dimensiones + INSERT fact con FK reales
- `_upsert_dim_tipo(cur, df)`, `_upsert_dim_distrito(cur, df)`, `_upsert_dim_estado(cur, df)`, `_upsert_dim_tiempo(cur, df)` — Upserts individuales por dimensión
- `_resolve_fact_ids(fact_df, map_tipo, map_distrito, map_estado)` — Resuelve FKs de llaves naturales a IDs reales de PostgreSQL
- `_upsert_fact(cur, fact_df)` — INSERT o UPDATE en FACT_EMERGENCIA

### `src/load/hash_to_storage.py` — Hash persistence

- `load_last_hash()` — Lee último hash de `pipeline_hash_log`
- `save_hash(hash_value)` — Inserta nuevo hash con pipeline_id y timestamp

### `src/utils/hashing.py` — Utilidad de hash

- `calculate_df_hash(data)` — SHA256 determinístico (ordena por NroParte)

### `src/utils/db.py` — Utilidad de base de datos

- `get_env(key)` — Lee variable de entorno con error descriptivo si falta
- `get_connection(max_retries, delay)` — Conexión a PostgreSQL con retry + backoff

### `dags/dag_accidents.py` — Orquestación Airflow

- DAG `pipeline_accidents` con `schedule=None` (trigger manual)
- 9 tasks con `retries=3` y `execution_timeout=300s`
- Tags: `["scraping", "bomberos"]`

---

## 4. Decisiones técnicas clave

| Decisión | Por qué |
|---|---|
| **CDC tipo SCD Type 2** | Mantiene historial completo de cambios de estado; `es_actual` + `estado_anterior` permiten reconstruir la línea de tiempo |
| **Hash SHA256 para skip** | Evita reprocesar datos idénticos; ahorra tiempo y recursos en ejecuciones repetidas |
| **Gold upsert con natural keys** | Transform retorna llaves naturales, loader resuelve FKs contra IDs reales de PostgreSQL — lógica no duplicada |
| **`get_env()` y `get_connection()` en `src/utils/db.py`** | Extraídas de `silver_to_storage.py` y `hash_to_storage.py` — ahora compartidas por todos los módulos de carga |
| **TEMP en `/opt/airflow/data/tmp/`** | Volumen compartido entre workers de Celery; asegura que cualquier worker pueda acceder al archivo temporal |
| **DAG manual (`schedule=None`)** | Los datos de Bomberos se actualizan en tiempo real sin horario fijo; el trigger manual permite decidir cuándo extraer |
| **Bronze en disco (no DB)** | Raw JSON preserva el original inmutable; si la transformación cambia, se reprocesa desde Bronze sin re-scrapear |

---

## 5. Estado actual — Code Quality Score

| Categoría | Score | Notas |
|---|---|---|
| Code Quality | 9/10 | Sin lógica duplicada; FK resuelta una sola vez |
| Architecture | 9/10 | Medallion bien separado; Star Schema correcto |
| Imports | 9/10 | Sin imports circulares; `gold_to_storage.py` limpio |
| Type Hints | 10/10 | 32/32 funciones con type hints (100%) |
| Docstrings | 8/10 | Buenos en español; DAG mezcla inglés/español (minoría) |
| Testing | 9/10 | 62 tests, 9/9 módulos cubiertos |
| Security | 8/10 | `.env` ignorado; sin secrets en código |
| Configuration | 8/10 | Paths externalizados a env vars; DB vars bien configuradas |
| **Overall** | **8.5/10** | Sólido para portafolio profesional |

---

## 6. Deuda técnica — estado actual

### 🔴 CRÍTICO — Resuelto

- [x] **`.env` en git** — ✅ Ya en `.gitignore`.
- [x] **`_get_env()` y `_get_connection()` a `src/utils/db.py`** — ✅ Duplicación eliminada.

### 🟡 ALTA — Resuelto

- [x] **Tests para `validate_bronze.py`** — 2 tests creados.
- [x] **Tests para `transform_gold.py`** — 14 tests creados.
- [x] **Tests para `gold_to_storage.py`** — 8 tests creados.
- [x] **Tests para `raw_to_storage.py`** — 3 tests creados.
- [x] **Tests para `transform_silver.py`** — 18 tests creados (prueban la transformación real).
- [x] **Renombrar `test_transform_silver.py`** → `tests/test_silver_to_storage.py`.

### 🔵 MEDIA — Resuelto

- [x] **Type hints en `transform_silver.py`** — 8/8 funciones, 100%.
- [x] **Type hints en `transform_gold.py`** — 6/6 funciones, 100%.
- [x] **Externalizar `BASE_PATH`** en `raw_to_storage.py` → `_BRONZE_PATH` via env var.
- [x] **Externalizar temp path** en `dag_accidents.py` → `BOMBEROS_TMP_DIR` via env var.
- [x] **FK resuelta dos veces** — Transform retorna llaves naturales, loader resuelve una sola vez.

### ⚪ BAJA — Resuelto

- [x] **Agregar `__init__.py`** a `src/load/`, `src/transform/` y `tests/`.
- [x] **Mover `import pandas`** fuera del DAG (eliminado, ya no se usaba).

### ⚪ BAJA — Pendiente (cosméticos)

- [ ] **Extraer constantes de turno** (`bins=[-1, 5, 11, 18, 23]` y labels) a constante de módulo.
- [ ] **Unificar idioma en docstrings del DAG** (mezcla español/inglés).
- [ ] **Corregir type hint de `normalize_newlines()`** — declarado como `str -> str` pero retorna `None`.
- [ ] **Limpiar imports no usados** (`get_env` en `silver_to_storage.py` y `hash_to_storage.py`).

---

## 7. Cómo trabajar

### Tests

```bash
uv run pytest tests/ -v        # Todos los tests (62)
uv run pytest tests/test_scraper.py -v  # Solo scraper
```

### Entorno local

```bash
cp .env.example .env          # Primera vez
docker compose up -d           # Levanta Airflow + Postgres + Redis
# Trigger manual del DAG en http://localhost:8080
```

### Package manager

```bash
uv sync                        # Instalar dependencias
uv add <package>               # Agregar dependencia
uv export --no-dev > requirements.txt  # Regenerar requirements para Docker
```

### Python

Versión: **3.14** (pinned en `.python-version`)

---

## 8. Git

- **Ramas activas**: `main` (producción), `develop` (integración), `feature/*` (tareas individuales).
- **Workflow**: feature → PR → develop → PR → main.
- **`.env` en `.gitignore`** — no committear.

---

## 9. Tests — estado actual

| Test file | Lo que prueba | Tests | Estado |
|---|---|---|---|
| `tests/test_hashing.py` | `hashing.py` | 4 | ✅ |
| `tests/test_scraper.py` | `scraper.py` | 4 | ✅ |
| `tests/test_hash_to_storage.py` | `hash_to_storage.py` | 3 | ✅ |
| `tests/test_silver_to_storage.py` | `silver_to_storage.py` | 7 | ✅ |
| `tests/test_transform_silver.py` | `transform_silver.py` | 18 | ✅ |
| `tests/test_transform_gold.py` | `transform_gold.py` | 14 | ✅ |
| `tests/test_validate_bronze.py` | `validate_bronze.py` | 2 | ✅ |
| `tests/test_gold_to_storage.py` | `gold_to_storage.py` | 8 | ✅ |
| `tests/test_raw_to_storage.py` | `raw_to_storage.py` | 3 | ✅ |
| **Total** | **9/9 módulos** | **62** | ✅ **100% pasando** |

---

## 10. Referencias rápidas

| Comando | Descripción |
|---|---|
| `uv run pytest tests/ -v` | Ejecutar todos los tests |
| `docker compose up -d` | Levantar stack completo |
| `docker compose down -v` | Bajar y borrar volúmenes |
| `docker compose logs -f airflow-scheduler` | Ver logs del scheduler |
| `uv export --no-dev > requirements.txt` | Regenerar requirements.txt |

### Archivos clave

| Archivo | Ruta |
|---|---|
| DAG principal | `dags/dag_accidents.py` |
| Config Airflow | `config/airflow.cfg` (no tocar) |
| Docker Compose | `docker-compose.yaml` |
| Dependencias | `pyproject.toml` |
| Scripts DB | `scripts/initdb/` |
| Env vars | `.env` (no committear) |
| Env template | `.env.example` |
