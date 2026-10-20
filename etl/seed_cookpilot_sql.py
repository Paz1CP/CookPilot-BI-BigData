
from __future__ import annotations

import sys
import argparse
import traceback
import calendar
import subprocess
import json
import re
import uuid
import random
import unicodedata
from pathlib import Path
from datetime import datetime, date, time, timedelta, timezone
from decimal import Decimal, InvalidOperation

DOWNLOADS = Path.home() / "Downloads"
PREFERRED_SQL_DUMP = DOWNLOADS / "backup_cookpilot_prod_2026-10-03_16-04-24.sql"
REPORT_PATH = DOWNLOADS / "cookpilot_etl_report.json"

SQL_SERVER = "localhost"
SQL_DATABASE = "CookPilot"

MOCK_USERS = 100
RANDOM_SEED = 20260923
START_DATE = date(2026, 3, 23)
END_DATE = date(2026, 9, 23)
DAYS_TO_PLAN = (END_DATE - START_DATE).days + 1

SOURCE_TABLES = {
    "menu.recipes",
    "menu.recipe_nutritional_profiles",
    "nutrition.ingredients",
    "nutrition.ingredient_categories",
    "menu.recipe_composition_ingredients",
    "menu.recipe_sections",
    "menu.recipe_steps",
    "menu.recipe_step_ingredients",
    "menu.recipe_time_estimates",
    "menu.recipe_categories",
    "menu.recipe_category_assignments",
    "pantry.tool_catalog",
}

TARGET_TABLES = [
    "users.profiles",
    "menu.recipes",
    "nutrition.ingredients",
    "menu.recipe_composition_ingredients",
    "menu.recipe_steps",
    "cookplan.plan_entries",
    "cooklist.lists",
    "cooklist.list_items",
    "cooklist.list_item_origins",
    "home.cookmode_sessions",
    "menu.recipe_categories",
    "menu.recipe_category_assignments",
    "menu.recipe_sections",
    "menu.recipe_step_ingredients",
    "menu.recipe_time_estimates",
    "users.user_preferences",
    "users.user_nutrition_goals",
    "social.recipe_likes",
    "pantry.tool_catalog",
    "cookplan.saved_items",
]

DELETE_ORDER = [
    "cooklist.list_item_origins",
    "cooklist.list_items",
    "cooklist.lists",
    "cookplan.saved_items",
    "home.cookmode_sessions",
    "cookplan.plan_entries",
    "social.recipe_likes",
    "users.user_nutrition_goals",
    "users.user_preferences",
    "menu.recipe_step_ingredients",
    "menu.recipe_steps",
    "menu.recipe_sections",
    "menu.recipe_category_assignments",
    "menu.recipe_time_estimates",
    "menu.recipe_composition_ingredients",
    "pantry.tool_catalog",
    "menu.recipe_categories",
    "menu.recipes",
    "nutrition.ingredients",
    "users.profiles",
]

DWH_TABLES = [
    "dbo.fact_planning", "dbo.fact_shopping_item", "dbo.fact_cooking_session",
    "dbo.dim_date", "dbo.dim_user", "dbo.dim_recipe", "dbo.dim_category",
    "dbo.dim_meal_moment", "dbo.dim_ingredient", "dbo.dim_shopping_list",
    "dbo.dim_purchase_status", "dbo.dim_session_status",
]

ALL_TABLES = TARGET_TABLES + DWH_TABLES

# La limpieza del almacén se ejecuta antes de las tablas operacionales.
RESET_ORDER = DWH_TABLES[:3] + DWH_TABLES[3:] + DELETE_ORDER

ALLOWED_UNITS = {"g", "kg", "oz", "lb", "ml", "l", "tsp", "tbsp", "cup", "unit"}
ALLOWED_INGREDIENT_CATEGORIES = {
    "cereals", "vegetables", "fruits", "fats", "fish_seafood", "meats",
    "dairy", "beverages", "eggs_derivatives", "sugary", "miscellaneous",
    "legumes", "tubers",
}

NAMESPACE = uuid.UUID("8fa9665d-f87c-4550-bbd8-417e37c84a34")
UNCATEGORIZED_CATEGORY_ID = "00000000-0000-0000-0000-000000000000"


def ensure_pyodbc():
    try:
        import pyodbc
        return pyodbc
    except ImportError:
        print("Instalando pyodbc...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyodbc"])
        import pyodbc
        return pyodbc


pyodbc = ensure_pyodbc()


def find_dump() -> Path:
    if PREFERRED_SQL_DUMP.exists():
        return PREFERRED_SQL_DUMP
    candidates = sorted(
        DOWNLOADS.glob("backup_cookpilot_dev_*.sql"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            "No encontré backup_cookpilot_dev_*.sql en Downloads."
        )
    return candidates[0]


def pg_unescape(value: str):
    if value == r"\N":
        return None

    out = []
    i = 0
    mapping = {
        "b": "\b",
        "f": "\f",
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "v": "\v",
        "\\": "\\",
    }

    while i < len(value):
        if value[i] != "\\":
            out.append(value[i])
            i += 1
            continue

        i += 1
        if i >= len(value):
            out.append("\\")
            break

        ch = value[i]

        if ch in mapping:
            out.append(mapping[ch])
            i += 1
        elif ch in "01234567":
            digits = ch
            i += 1
            while i < len(value) and len(digits) < 3 and value[i] in "01234567":
                digits += value[i]
                i += 1
            out.append(chr(int(digits, 8)))
        elif ch == "x":
            i += 1
            digits = ""
            while i < len(value) and len(digits) < 2 and value[i] in "0123456789abcdefABCDEF":
                digits += value[i]
                i += 1
            out.append(chr(int(digits, 16)) if digits else "x")
        else:
            out.append(ch)
            i += 1

    return "".join(out)


def parse_copy_dump(path: Path):
    data = {name: [] for name in SOURCE_TABLES}
    copy_re = re.compile(r"^COPY\s+(.+?)\s+\((.*?)\)\s+FROM stdin;$")

    current_table = None
    columns = None
    keep = False

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\r\n")

            if current_table is None:
                match = copy_re.match(line)
                if not match:
                    continue

                table_ref = match.group(1).replace('"', "")
                current_table = table_ref
                columns = [c.strip().strip('"') for c in match.group(2).split(",")]
                keep = current_table in SOURCE_TABLES
                continue

            if line == r"\.":
                current_table = None
                columns = None
                keep = False
                continue

            if not keep:
                continue

            values = [pg_unescape(v) for v in line.split("\t")]

            if len(values) != len(columns):
                raise ValueError(
                    f"Fila COPY inválida en {current_table}: "
                    f"{len(values)} valores para {len(columns)} columnas."
                )

            data[current_table].append(dict(zip(columns, values)))

    return data


def parse_decimal(value, default=None):
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return default


def parse_int(value, default=None):
    if value is None or value == "":
        return default
    try:
        return int(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return default


def parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"t", "true", "1", "yes", "y"}


def parse_dt(value, default=None):
    if value is None or value == "":
        return default

    text = str(value).strip().replace(" ", "T", 1)

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return default

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    return dt


def parse_pg_array(value):
    if value is None:
        return []

    value = value.strip()

    if value == "{}":
        return []

    if not (value.startswith("{") and value.endswith("}")):
        return []

    body = value[1:-1]
    if not body:
        return []

    items = []
    current = []
    quoted = False
    escaped = False

    for ch in body:
        if escaped:
            current.append(ch)
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == '"':
            quoted = not quoted
        elif ch == "," and not quoted:
            items.append("".join(current))
            current = []
        else:
            current.append(ch)

    items.append("".join(current))
    return [x for x in items if x and x != "NULL"]


def normalize_text(value: str) -> str:
    value = value or ""
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return value.lower().strip()


def slugify(value: str) -> str:
    value = normalize_text(value)
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "item"


def ingredient_category_for(row, source_categories):
    source = source_categories.get(row.get("category_id"))
    text = ""

    if source:
        text = " ".join(
            filter(None, [source.get("code"), source.get("name_es"), source.get("name_en")])
        )

    text = normalize_text(text)

    mapping = [
        ("fish_seafood", ["fish", "seafood", "pescad", "marisc"]),
        ("eggs_derivatives", ["egg", "huevo"]),
        ("vegetables", ["vegetable", "verdura", "hortaliza"]),
        ("fruits", ["fruit", "fruta"]),
        ("cereals", ["cereal", "grain", "grano", "arroz", "harina"]),
        ("fats", ["fat", "oil", "aceite", "grasa"]),
        ("meats", ["meat", "carne", "pollo", "poultry"]),
        ("dairy", ["dairy", "lacteo", "leche", "queso"]),
        ("beverages", ["beverage", "drink", "bebida"]),
        ("sugary", ["sugar", "sweet", "azucar", "dulce"]),
        ("legumes", ["legume", "legumbre", "bean", "frijol", "frejol"]),
        ("tubers", ["tuber", "tuberculo", "papa", "potato"]),
    ]

    for target, tokens in mapping:
        if any(token in text for token in tokens):
            return target

    return "miscellaneous"


def base_unit_for(row):
    basis = (row.get("canonical_price_basis_type") or "").lower()

    if basis == "per_100ml":
        return "ml"
    if basis == "per_unit":
        return "unit"
    return "g"


def unique_value(value, seen, suffix):
    candidate = value
    key = candidate.casefold()

    if key not in seen:
        seen.add(key)
        return candidate

    candidate = f"{value}-{suffix}"
    n = 2

    while candidate.casefold() in seen:
        candidate = f"{value}-{suffix}-{n}"
        n += 1

    seen.add(candidate.casefold())
    return candidate


def clamp_decimal(value, low, high):
    value = parse_decimal(value, Decimal("1"))
    if value < low:
        return low
    if value > high:
        return high
    return value


def pick_driver():
    drivers = pyodbc.drivers()

    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]

    for driver in preferred:
        if driver in drivers:
            return driver

    raise RuntimeError(
        "No encontré un driver ODBC para SQL Server. "
        f"Drivers detectados: {drivers}"
    )


def connect_sql_server():
    driver = pick_driver()

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str, autocommit=False)


def insert_many(cursor, sql, rows, batch_size=1000):
    if not rows:
        return

    cursor.fast_executemany = True

    for start in range(0, len(rows), batch_size):
        cursor.executemany(sql, rows[start:start + batch_size])


def verify_target_schema(cursor):
    cursor.execute("""
        SELECT TABLE_SCHEMA + '.' + TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
    """)

    existing = {row[0] for row in cursor.fetchall()}
    missing = [table for table in TARGET_TABLES if table not in existing]

    if missing:
        raise RuntimeError(
            "Faltan tablas en CookPilot. Ejecuta primero las migrations 01 y 02.\n"
            + "\n".join(f"- {table}" for table in missing)
        )


# Esquema esperado: estas columnas y relaciones deben existir antes de tocar datos.
REQUIRED_COLUMNS = {
    "nutrition.ingredients": "id name base_unit category created_at updated_at",
    "menu.recipes": "id title canonical_name description cover_photo_url servings calories protein_g carbs_g fat_g created_at updated_at",
    "menu.recipe_categories": "id parent_id name name_en slug sort_order is_dynamic cover_photo_url created_at updated_at",
    "menu.recipe_category_assignments": "recipe_id category_id created_at",
    "menu.recipe_sections": "id recipe_id title title_en description description_en section_order source_section_id created_at",
    "menu.recipe_composition_ingredients": "id recipe_id ingredient_id quantity unit",
    "menu.recipe_steps": "id recipe_id step_number instruction section_id",
    "menu.recipe_step_ingredients": "id step_id rci_id fraction_ratio created_at updated_at",
    "menu.recipe_time_estimates": "recipe_id total_minutes_avg active_minutes_avg passive_minutes_avg optional_passive_minutes last_updated_at",
    "pantry.tool_catalog": "id code name_es name_en local_name_pe category criticality_index kit_min_level pe_prevalence is_traditional notes icon_path created_at",
    "users.profiles": "id username full_name created_at updated_at",
    "users.user_preferences": "user_id result_type available_budget_min_pen available_budget_max_pen available_time_min available_time_max cooking_for personal_goal currency created_at updated_at",
    "users.user_nutrition_goals": "user_id is_active goal_type current_weight_kg target_weight_kg height_cm age_years sex activity_level final_target_kcal protein_g carbs_g fat_g fiber_g created_at updated_at",
    "social.recipe_likes": "user_id recipe_id created_at",
    "cookplan.plan_entries": "id user_id recipe_id day_date slot_key servings created_at updated_at",
    "cookplan.saved_items": "id user_id item_type title content_snapshot nutrition_snapshot origin_meal_moment archived_at deleted_at created_at updated_at",
    "cooklist.lists": "id user_id title start_date end_date created_at updated_at",
    "cooklist.list_items": "id list_id ingredient_id quantity unit is_checked",
    "cooklist.list_item_origins": "list_item_id plan_entry_id",
    "home.cookmode_sessions": "id user_id recipe_id status current_step servings created_at updated_at finished_at",
    "dbo.dim_date": "date_key full_date day_number month_number month_name quarter semester year week_of_year",
    "dbo.dim_user": "user_key user_id username result_type cooking_for personal_goal nutrition_goal_type activity_level available_time_min available_time_max target_kcal target_protein_g target_carbs_g target_fat_g",
    "dbo.dim_recipe": "recipe_key recipe_id title canonical_name base_servings",
    "dbo.dim_category": "category_key category_id category_name parent_category",
    "dbo.dim_meal_moment": "meal_moment_key slot_key meal_moment_name sort_order",
    "dbo.dim_ingredient": "ingredient_key ingredient_id ingredient_name ingredient_category base_unit",
    "dbo.dim_shopping_list": "shopping_list_key list_id title planning_days",
    "dbo.dim_purchase_status": "purchase_status_key status_code status_name",
    "dbo.dim_session_status": "session_status_key status_code status_name is_completed",
    "dbo.fact_planning": "planning_fact_key date_key user_key recipe_key category_key meal_moment_key plan_entry_id planned_servings planned_calories planned_protein_g planned_carbs_g planned_fat_g estimated_minutes plan_count",
    "dbo.fact_shopping_item": "shopping_fact_key date_key user_key ingredient_key shopping_list_key purchase_status_key list_item_id required_quantity item_count purchased_item_count",
    "dbo.fact_cooking_session": "cooking_fact_key date_key user_key recipe_key category_key session_status_key session_id servings last_step_reached actual_duration_min estimated_duration_min duration_variance_min session_count completed_count",
}

# Tabla hija, columna hija, tabla padre, columna padre.
REQUIRED_FKS = [
    ("menu.recipe_composition_ingredients", "recipe_id", "menu.recipes", "id"),
    ("menu.recipe_composition_ingredients", "ingredient_id", "nutrition.ingredients", "id"),
    ("menu.recipe_steps", "recipe_id", "menu.recipes", "id"),
    ("menu.recipe_steps", "section_id", "menu.recipe_sections", "id"),
    ("menu.recipe_sections", "recipe_id", "menu.recipes", "id"),
    ("menu.recipe_step_ingredients", "step_id", "menu.recipe_steps", "id"),
    ("menu.recipe_step_ingredients", "rci_id", "menu.recipe_composition_ingredients", "id"),
    ("menu.recipe_time_estimates", "recipe_id", "menu.recipes", "id"),
    ("menu.recipe_category_assignments", "recipe_id", "menu.recipes", "id"),
    ("menu.recipe_category_assignments", "category_id", "menu.recipe_categories", "id"),
    ("users.user_preferences", "user_id", "users.profiles", "id"),
    ("users.user_nutrition_goals", "user_id", "users.profiles", "id"),
    ("social.recipe_likes", "user_id", "users.profiles", "id"),
    ("social.recipe_likes", "recipe_id", "menu.recipes", "id"),
    ("cookplan.plan_entries", "user_id", "users.profiles", "id"),
    ("cookplan.plan_entries", "recipe_id", "menu.recipes", "id"),
    ("cookplan.saved_items", "user_id", "users.profiles", "id"),
    ("cooklist.lists", "user_id", "users.profiles", "id"),
    ("cooklist.list_items", "list_id", "cooklist.lists", "id"),
    ("cooklist.list_items", "ingredient_id", "nutrition.ingredients", "id"),
    ("cooklist.list_item_origins", "list_item_id", "cooklist.list_items", "id"),
    ("cooklist.list_item_origins", "plan_entry_id", "cookplan.plan_entries", "id"),
    ("home.cookmode_sessions", "user_id", "users.profiles", "id"),
    ("home.cookmode_sessions", "recipe_id", "menu.recipes", "id"),
    ("dbo.fact_planning", "date_key", "dbo.dim_date", "date_key"),
    ("dbo.fact_planning", "user_key", "dbo.dim_user", "user_key"),
    ("dbo.fact_planning", "recipe_key", "dbo.dim_recipe", "recipe_key"),
    ("dbo.fact_planning", "category_key", "dbo.dim_category", "category_key"),
    ("dbo.fact_planning", "meal_moment_key", "dbo.dim_meal_moment", "meal_moment_key"),
    ("dbo.fact_shopping_item", "date_key", "dbo.dim_date", "date_key"),
    ("dbo.fact_shopping_item", "user_key", "dbo.dim_user", "user_key"),
    ("dbo.fact_shopping_item", "ingredient_key", "dbo.dim_ingredient", "ingredient_key"),
    ("dbo.fact_shopping_item", "shopping_list_key", "dbo.dim_shopping_list", "shopping_list_key"),
    ("dbo.fact_shopping_item", "purchase_status_key", "dbo.dim_purchase_status", "purchase_status_key"),
    ("dbo.fact_cooking_session", "date_key", "dbo.dim_date", "date_key"),
    ("dbo.fact_cooking_session", "user_key", "dbo.dim_user", "user_key"),
    ("dbo.fact_cooking_session", "recipe_key", "dbo.dim_recipe", "recipe_key"),
    ("dbo.fact_cooking_session", "category_key", "dbo.dim_category", "category_key"),
    ("dbo.fact_cooking_session", "session_status_key", "dbo.dim_session_status", "session_status_key"),
]


def verify_full_schema(cursor):
    verify_target_schema(cursor)
    cursor.execute("""
        SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS tbl,
               c.name, c.max_length, c.is_identity,
               TYPE_NAME(c.user_type_id) AS sql_type
        FROM sys.tables t JOIN sys.columns c ON t.object_id = c.object_id
    """)
    meta = {}
    for tbl, col, length, identity, data_type in cursor.fetchall():
        meta.setdefault(tbl, {})[col] = (length, bool(identity), data_type)
    problems = []
    for tbl, cols in REQUIRED_COLUMNS.items():
        if tbl not in meta:
            problems.append(f"Falta tabla: {tbl}")
            continue
        for col in cols.split():
            if col not in meta[tbl]:
                problems.append(f"Falta columna: {tbl}.{col}")
    for tbl, col in [("dbo.dim_user", "user_id"), ("dbo.dim_recipe", "recipe_id"),
                     ("dbo.dim_ingredient", "ingredient_id"),
                     ("dbo.dim_category", "category_id"),
                     ("dbo.dim_shopping_list", "list_id")]:
        if col in meta.get(tbl, {}):
            length, _, typ = meta[tbl][col]
            if typ.lower() not in {"varchar", "nvarchar", "char", "nchar"}:
                problems.append(f"{tbl}.{col} debe ser una columna de texto para UUID de 36 caracteres")
            elif length != -1 and (length // (2 if typ.lower().startswith('n') else 1)) < 36:
                problems.append(f"{tbl}.{col} acepta menos de 36 caracteres: UUID completo requiere 36")

    # FK presentes Y habilitadas. Una FK ausente o deshabilitada hace fallar la carga.
    cursor.execute("""
        SELECT SCHEMA_NAME(c.schema_id)+'.'+c.name, pc.name,
               SCHEMA_NAME(r.schema_id)+'.'+r.name, rc.name,
               fk.is_disabled, fk.is_not_trusted
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id
        JOIN sys.tables c ON c.object_id=fkc.parent_object_id
        JOIN sys.columns pc ON pc.object_id=c.object_id AND pc.column_id=fkc.parent_column_id
        JOIN sys.tables r ON r.object_id=fkc.referenced_object_id
        JOIN sys.columns rc ON rc.object_id=r.object_id AND rc.column_id=fkc.referenced_column_id
    """)
    fks = {(a,b,c,d):(disabled,untrusted) for a,b,c,d,disabled,untrusted in cursor.fetchall()}
    for fk in REQUIRED_FKS:
        if fk not in fks:
            problems.append("Falta relación FK: {}.{} -> {}.{}".format(*fk))
        elif any(fks[fk]):
            problems.append("Relación FK deshabilitada/no confiable: {}.{} -> {}.{}".format(*fk))

    cursor.execute("""
        SELECT SCHEMA_NAME(t.schema_id)+'.'+t.name AS tbl, col.name
        FROM sys.key_constraints k
        JOIN sys.tables t ON k.parent_object_id=t.object_id
        JOIN sys.index_columns ic ON ic.object_id=t.object_id AND ic.index_id=k.unique_index_id
        JOIN sys.columns col ON col.object_id=t.object_id AND col.column_id=ic.column_id
        WHERE k.type = 'PK'
    """)
    pk_tables = {row[0] for row in cursor.fetchall()}
    for tbl in ALL_TABLES:
        if tbl not in pk_tables:
            problems.append(f"Sin clave primaria: {tbl}")
    if "date_key" in meta.get("dbo.dim_date", {}) and meta["dbo.dim_date"]["date_key"][1]:
        problems.append("dbo.dim_date.date_key no debe ser IDENTITY; se carga como YYYYMMDD")
    if problems:
        raise RuntimeError("Esquema incorrecto. No se borró ni cargó ningún registro:\n - " + "\n - ".join(problems))
    return meta


def clear_target(cursor):
    for table in RESET_ORDER:
        cursor.execute(f"DELETE FROM {table};")


def transform_source(src):
    now = datetime(2026, 9, 23, 12, 0, 0)

    source_categories = {
        row["id"]: row
        for row in src.get("nutrition.ingredient_categories", [])
        if row.get("id")
    }

    ingredient_rows = []
    ingredient_ids = set()
    ingredient_base_unit = {}
    seen_ingredient_names = set()

    for row in src["nutrition.ingredients"]:
        ingredient_id = row.get("id")
        name = (row.get("name") or "").strip()

        if not ingredient_id or not name:
            continue

        name = unique_value(name, seen_ingredient_names, ingredient_id[:8])
        base_unit = base_unit_for(row)
        category = ingredient_category_for(row, source_categories)

        if category not in ALLOWED_INGREDIENT_CATEGORIES:
            category = "miscellaneous"

        ingredient_rows.append((
            ingredient_id,
            name[:150],
            base_unit,
            category,
            parse_dt(row.get("created_at"), now),
            parse_dt(row.get("updated_at"), now),
        ))

        ingredient_ids.add(ingredient_id)
        ingredient_base_unit[ingredient_id] = base_unit

    nutrition_profiles = {
        row["recipe_id"]: row
        for row in src["menu.recipe_nutritional_profiles"]
        if row.get("recipe_id")
    }

    recipe_rows = []
    recipe_ids = set()
    recipe_base_servings = {}
    recipe_title = {}
    seen_canonical = set()

    for row in src["menu.recipes"]:
        recipe_id = row.get("id")
        title = (row.get("title") or "").strip()

        if not recipe_id or not title:
            continue

        canonical = (row.get("canonical_name") or slugify(title)).strip()
        canonical = unique_value(canonical, seen_canonical, recipe_id[:8])

        servings = parse_int(row.get("servings"), 4)
        if servings is None or servings <= 0:
            servings = 4

        profile = nutrition_profiles.get(recipe_id, {})

        calories = parse_int(profile.get("calories"))
        if calories is not None and calories < 0:
            calories = 0

        def nonnegative_decimal(value):
            parsed = parse_decimal(value)
            if parsed is not None and parsed < 0:
                return Decimal("0")
            return parsed

        recipe_rows.append((
            recipe_id,
            title[:200],
            canonical[:200],
            (row.get("description") or title),
            (row.get("cover_photo_url") or None),
            servings,
            calories,
            nonnegative_decimal(profile.get("protein_g")),
            nonnegative_decimal(profile.get("carbs_g")),
            nonnegative_decimal(profile.get("fat_g")),
            parse_dt(row.get("created_at"), now),
            parse_dt(row.get("updated_at"), now),
        ))

        recipe_ids.add(recipe_id)
        recipe_base_servings[recipe_id] = servings
        recipe_title[recipe_id] = title[:200]

    grouped_rci = {}
    rci_source_to_target = {}

    for row in src["menu.recipe_composition_ingredients"]:
        source_id = row.get("id")
        recipe_id = row.get("recipe_id")
        ingredient_id = row.get("ingredient_id")

        if (
            not source_id
            or recipe_id not in recipe_ids
            or ingredient_id not in ingredient_ids
        ):
            continue

        quantity = parse_decimal(row.get("quantity"))
        if quantity is None or quantity <= 0:
            quantity = parse_decimal(row.get("display_quantity"))

        if quantity is None or quantity <= 0:
            continue

        unit = (row.get("unit") or row.get("display_unit") or ingredient_base_unit.get(ingredient_id) or "g").lower()

        if unit not in ALLOWED_UNITS:
            unit = ingredient_base_unit.get(ingredient_id, "g")

        key = (recipe_id, ingredient_id, unit)

        if key not in grouped_rci:
            grouped_rci[key] = {
                "id": source_id,
                "recipe_id": recipe_id,
                "ingredient_id": ingredient_id,
                "quantity": Decimal("0"),
                "unit": unit,
            }

        grouped_rci[key]["quantity"] += quantity
        rci_source_to_target[source_id] = grouped_rci[key]["id"]

    rci_rows = [
        (
            item["id"],
            item["recipe_id"],
            item["ingredient_id"],
            item["quantity"],
            item["unit"],
        )
        for item in grouped_rci.values()
    ]

    rci_ids = {row[0] for row in rci_rows}

    section_source = [
        row for row in src["menu.recipe_sections"]
        if row.get("id") and row.get("recipe_id") in recipe_ids
    ]

    section_rows = []
    section_ids = set()
    section_source_links = []

    by_recipe_sections = {}
    for row in section_source:
        by_recipe_sections.setdefault(row["recipe_id"], []).append(row)

    for recipe_id, rows in by_recipe_sections.items():
        rows.sort(key=lambda r: (parse_int(r.get("section_order"), 0), r["id"]))
        used_orders = set()

        for row in rows:
            order = max(parse_int(row.get("section_order"), 0), 0)
            while order in used_orders:
                order += 1
            used_orders.add(order)

            title = (row.get("title") or "").strip()
            if not title:
                title = f"Section {order + 1}"

            section_rows.append((
                row["id"],
                recipe_id,
                title[:150],
                (row.get("title_en") or None),
                (row.get("description") or None),
                (row.get("description_en") or None),
                order,
                None,
                parse_dt(row.get("created_at"), now),
            ))

            section_ids.add(row["id"])

            if row.get("source_section_id"):
                section_source_links.append((row["source_section_id"], row["id"]))

    step_rows = []
    step_ids = set()
    seen_steps = set()

    for row in sorted(
        src["menu.recipe_steps"],
        key=lambda r: (
            r.get("recipe_id") or "",
            parse_int(r.get("step_number"), 0),
            r.get("id") or "",
        ),
    ):
        step_id = row.get("id")
        recipe_id = row.get("recipe_id")
        step_number = parse_int(row.get("step_number"))
        instruction = (row.get("instruction") or "").strip()

        if (
            not step_id
            or recipe_id not in recipe_ids
            or step_number is None
            or step_number <= 0
            or not instruction
        ):
            continue

        key = (recipe_id, step_number)
        if key in seen_steps:
            continue
        seen_steps.add(key)

        section_id = row.get("section_id")
        if section_id not in section_ids:
            section_id = None

        step_rows.append((
            step_id,
            recipe_id,
            step_number,
            instruction,
            section_id,
        ))
        step_ids.add(step_id)

    grouped_rsi = {}

    for row in src["menu.recipe_step_ingredients"]:
        step_id = row.get("step_id")
        source_rci_id = row.get("rci_id")
        target_rci_id = rci_source_to_target.get(source_rci_id)

        if step_id not in step_ids or target_rci_id not in rci_ids:
            continue

        key = (step_id, target_rci_id)
        ratio = clamp_decimal(
            row.get("fraction_ratio"),
            Decimal("0.000001"),
            Decimal("1"),
        )

        if key not in grouped_rsi:
            grouped_rsi[key] = {
                "id": row.get("id") or str(uuid.uuid5(NAMESPACE, f"rsi:{step_id}:{target_rci_id}")),
                "ratio": Decimal("0"),
            }

        grouped_rsi[key]["ratio"] += ratio
        if grouped_rsi[key]["ratio"] > Decimal("1"):
            grouped_rsi[key]["ratio"] = Decimal("1")

    rsi_rows = [
        (item["id"], step_id, rci_id, item["ratio"], now, now)
        for (step_id, rci_id), item in grouped_rsi.items()
    ]

    time_rows = []
    for row in src["menu.recipe_time_estimates"]:
        recipe_id = row.get("recipe_id")
        if recipe_id not in recipe_ids:
            continue

        total = max(parse_int(row.get("total_minutes_avg"), 0), 0)
        active = max(parse_int(row.get("active_minutes_avg"), 0), 0)
        passive = max(parse_int(row.get("passive_minutes_avg"), 0), 0)
        optional = parse_int(row.get("optional_passive_minutes"))

        if optional is not None:
            optional = max(optional, 0)

        time_rows.append((
            recipe_id,
            total,
            active,
            passive,
            optional,
            parse_dt(row.get("last_updated_at"), now),
        ))

    category_rows = []
    category_ids = set()
    category_parent_links = []
    seen_slugs = set()

    for row in src["menu.recipe_categories"]:
        category_id = row.get("id")
        name = (row.get("name") or "").strip()
        slug = (row.get("slug") or slugify(name)).strip()

        if not category_id or not name:
            continue

        slug = unique_value(slug, seen_slugs, category_id[:8])

        category_rows.append((
            category_id,
            None,
            name[:120],
            (row.get("name_en") or None),
            slug[:140],
            max(parse_int(row.get("sort_order"), 0), 0),
            parse_bool(row.get("is_dynamic"), False),
            (row.get("cover_photo_url") or None),
            parse_dt(row.get("created_at"), now),
            parse_dt(row.get("updated_at"), now),
        ))

        category_ids.add(category_id)

        if row.get("parent_id"):
            category_parent_links.append((row["parent_id"], category_id))

    category_assignment_rows = []
    seen_assignments = set()

    for row in src["menu.recipe_category_assignments"]:
        recipe_id = row.get("recipe_id")
        if recipe_id not in recipe_ids:
            continue

        for category_id in parse_pg_array(row.get("category_ids")):
            if category_id not in category_ids:
                continue

            key = (recipe_id, category_id)
            if key in seen_assignments:
                continue

            seen_assignments.add(key)
            category_assignment_rows.append((
                recipe_id,
                category_id,
                parse_dt(row.get("created_at"), now),
            ))

    tool_rows = []
    seen_codes = set()

    for row in src["pantry.tool_catalog"]:
        tool_id = row.get("id")
        code = (row.get("code") or "").strip()
        name_es = (row.get("name_es") or "").strip()

        if not tool_id or not code or not name_es:
            continue

        if code.casefold() in seen_codes:
            continue
        seen_codes.add(code.casefold())

        category = row.get("category")
        if category not in {
            "cutting", "prep", "cooking", "baking", "appliance", "storage",
            "cleaning", "safety", "grill", "beverage", "service", "improvised", "other"
        }:
            category = None

        criticality = parse_int(row.get("criticality_index"))
        if criticality is not None and not (1 <= criticality <= 5):
            criticality = None

        kit = parse_int(row.get("kit_min_level"))
        if kit is not None and not (1 <= kit <= 4):
            kit = None

        prevalence = row.get("pe_prevalence")
        if prevalence not in {"common", "moderate", "rare", "niche"}:
            prevalence = None

        tool_rows.append((
            tool_id,
            code[:60],
            name_es[:120],
            (row.get("name_en") or None),
            (row.get("local_name_pe") or None),
            category,
            criticality,
            kit,
            prevalence,
            parse_bool(row.get("is_traditional"), False),
            (row.get("notes") or None),
            (row.get("icon_path") or None),
            parse_dt(row.get("created_at"), now),
        ))

    if not recipe_rows:
        raise RuntimeError("El dump no contiene menu.recipes.")
    if not ingredient_rows:
        raise RuntimeError("El dump no contiene nutrition.ingredients.")
    if not category_rows:
        raise RuntimeError("El dump no contiene menu.recipe_categories.")
    if not tool_rows:
        raise RuntimeError("El dump no contiene pantry.tool_catalog.")

    return {
        "ingredients": ingredient_rows,
        "recipes": recipe_rows,
        "rci": rci_rows,
        "sections": section_rows,
        "section_source_links": section_source_links,
        "steps": step_rows,
        "rsi": rsi_rows,
        "times": time_rows,
        "categories": category_rows,
        "category_parent_links": category_parent_links,
        "category_assignments": category_assignment_rows,
        "tools": tool_rows,
        "recipe_ids": list(recipe_ids),
        "recipe_base_servings": recipe_base_servings,
        "recipe_title": recipe_title,
    }


def build_mock_data(transformed):
    rng = random.Random(RANDOM_SEED)
    now = datetime.combine(END_DATE, time(12, 0))

    first_names = [
        "Ana", "Luis", "María", "Diego", "Camila", "Jorge", "Lucía", "Mateo",
        "Valeria", "Andrés", "Sofía", "Carlos", "Daniela", "Renato", "Fernanda",
        "Alonso", "Paula", "Sebastián", "Elena", "Bruno",
    ]
    last_names = [
        "García", "Rodríguez", "Torres", "Flores", "Vargas", "Rojas", "Mendoza",
        "Castillo", "Salazar", "Chávez", "Paredes", "Ramírez", "Cruz", "Reyes",
        "Sánchez", "Campos", "Medina", "Aguilar", "Herrera", "Navarro",
    ]

    users = []
    preferences = []
    goals = []
    user_ids = []

    for i in range(1, MOCK_USERS + 1):
        user_id = str(uuid.uuid5(NAMESPACE, f"user:{i}"))
        user_ids.append(user_id)

        first = first_names[(i - 1) % len(first_names)]
        last = last_names[((i - 1) * 7) % len(last_names)]
        username = f"{normalize_text(first)}.{normalize_text(last)}{i:03d}".replace(" ", "")
        full_name = f"{first} {last}"
        created = datetime.combine(START_DATE, time(0, 0)) - timedelta(days=rng.randint(7, 180))

        users.append((user_id, username[:50], full_name[:120], created, created))

        budget_min = Decimal(str(rng.choice([20, 25, 30, 35, 40, 50])))
        budget_max = budget_min + Decimal(str(rng.choice([10, 20, 30, 40, 50])))
        time_min = rng.choice([15, 20, 30, 45])
        time_max = rng.choice([60, 75, 90, 120])

        if time_max < time_min:
            time_max = time_min

        preferences.append((
            user_id,
            rng.choice(["one_dish", "simple_menu", "full_menu", "variety"]),
            budget_min,
            budget_max,
            time_min,
            time_max,
            rng.choice(["one_person", "two_people", "three_four_people", "more_people"]),
            rng.choice(["lose_fat", "maintain", "gain_weight"]),
            "PEN",
            created,
            created,
        ))

        sex = rng.choice(["male", "female"])
        age = rng.randint(18, 60)
        height = Decimal(str(rng.randint(150, 190)))
        weight = Decimal(str(rng.randint(50, 100)))
        goal_type = rng.choice(["lose", "maintain", "gain", "performance", "healthy"])
        target = None if goal_type in {"maintain", "healthy"} else weight + Decimal(str(rng.choice([-8, -5, 5, 8])))
        activity = rng.choice(["sedentary", "light", "moderate", "active", "veryActive"])
        kcal = rng.randint(1700, 3200)
        protein = rng.randint(90, 190)
        fat = rng.randint(45, 100)
        carbs = max(100, int((kcal - protein * 4 - fat * 9) / 4))
        fiber = rng.randint(20, 40)

        goals.append((
            user_id, 1, goal_type, weight, target, height, age, sex, activity,
            kcal, protein, carbs, fat, fiber, created, created
        ))

    recipe_ids = transformed["recipe_ids"]
    recipe_title = transformed["recipe_title"]
    recipe_base_servings = transformed["recipe_base_servings"]

    likes = []
    for user_id in user_ids:
        count = min(rng.randint(8, 20), len(recipe_ids))
        for recipe_id in rng.sample(recipe_ids, count):
            likes.append((user_id, recipe_id, now - timedelta(days=rng.randint(0, 180))))

    slots = ["breakfast", "morning_snack", "lunch", "afternoon_snack", "dinner", "late_night"]
    primary_slots = ["breakfast", "lunch", "dinner"]
    plans = []

    for user_index, user_id in enumerate(user_ids):
        # No todos los usuarios planifican todos los días: permite observar recurrencia.
        activity_probability = rng.uniform(0.40, 0.85)
        inactivity_month = rng.choice([None, None, None, None, 5, 6])
        for offset in range(DAYS_TO_PLAN):
            day = START_DATE + timedelta(days=offset)
            months_since_start = (day.year - START_DATE.year) * 12 + day.month - START_DATE.month
            if (inactivity_month is not None and months_since_start >= inactivity_month
                    and not (user_index == 0 and offset == DAYS_TO_PLAN - 1)):
                continue
            if offset not in (0, DAYS_TO_PLAN - 1) and rng.random() > activity_probability:
                continue
            chosen_slots = rng.sample(primary_slots, 2 if rng.random() < 0.82 else 3)
            if rng.random() < 0.12:
                chosen_slots.append(rng.choice(["morning_snack", "afternoon_snack"]))

            for slot in chosen_slots:
                recipe_id = rng.choice(recipe_ids)
                servings = rng.randint(1, 4)
                plan_id = str(uuid.uuid5(
                    NAMESPACE,
                    f"plan:{user_id}:{day.isoformat()}:{slot}:{recipe_id}"
                ))
                created = datetime.combine(day, time(8, 0))

                plans.append((
                    plan_id,
                    user_id,
                    recipe_id,
                    day,
                    slot,
                    servings,
                    created,
                    created,
                ))

    composition_by_recipe = {}
    for rci_id, recipe_id, ingredient_id, quantity, unit in transformed["rci"]:
        composition_by_recipe.setdefault(recipe_id, []).append(
            (ingredient_id, Decimal(quantity), unit)
        )

    plans_by_user = {}
    for plan in plans:
        plans_by_user.setdefault(plan[1], []).append(plan)

    shopping_lists = []
    list_items = []
    origins = []

    for user_id in user_ids:
        user_plans = plans_by_user.get(user_id, [])

        for week_index in range((DAYS_TO_PLAN + 6) // 7):
            start = START_DATE + timedelta(days=week_index * 7)
            end = min(start + timedelta(days=6), END_DATE)

            week_plans = [p for p in user_plans if start <= p[3] <= end]
            if not week_plans:
                continue

            list_id = str(uuid.uuid5(NAMESPACE, f"list:{user_id}:{start.isoformat()}"))
            created = datetime.combine(start, time(7, 0))

            shopping_lists.append((
                list_id,
                user_id,
                f"Lista semanal {week_index + 1}",
                start,
                end,
                created,
                created,
            ))

            aggregate = {}

            for plan in week_plans:
                plan_id, _, recipe_id, _, _, planned_servings, _, _ = plan
                base_servings = max(recipe_base_servings.get(recipe_id, 1), 1)
                factor = Decimal(planned_servings) / Decimal(base_servings)

                for ingredient_id, quantity, unit in composition_by_recipe.get(recipe_id, []):
                    key = (ingredient_id, unit)

                    if key not in aggregate:
                        aggregate[key] = {
                            "quantity": Decimal("0"),
                            "origins": set(),
                        }

                    aggregate[key]["quantity"] += quantity * factor
                    aggregate[key]["origins"].add(plan_id)

            for (ingredient_id, unit), data in aggregate.items():
                quantity = data["quantity"].quantize(Decimal("0.001"))

                if quantity <= 0:
                    continue

                item_id = str(uuid.uuid5(
                    NAMESPACE,
                    f"list-item:{list_id}:{ingredient_id}:{unit}"
                ))

                list_items.append((
                    item_id,
                    list_id,
                    ingredient_id,
                    quantity,
                    unit,
                    1 if rng.random() < 0.65 else 0,
                ))

                for plan_id in sorted(data["origins"]):
                    origins.append((item_id, plan_id))

    step_count_by_recipe = {}
    for _, recipe_id, step_number, _, _ in transformed["steps"]:
        step_count_by_recipe[recipe_id] = max(
            step_count_by_recipe.get(recipe_id, 0),
            step_number,
        )

    sessions = []
    for plan in plans:
        plan_id, user_id, recipe_id, day, slot, servings, _, _ = plan

        if not (user_id == user_ids[0] and day in (START_DATE, END_DATE)) and rng.random() > 0.55:
            continue

        max_step = max(step_count_by_recipe.get(recipe_id, 1), 1)
        finished = rng.random() < 0.82
        status = "finished" if finished else "abandoned"
        current_step = max_step if finished else rng.randint(1, max_step)

        session_id = str(uuid.uuid5(NAMESPACE, f"session:{plan_id}"))
        created = datetime.combine(day, time(rng.randint(7, 20), rng.choice([0, 15, 30, 45])))
        finished_at = created + timedelta(minutes=rng.randint(15, 120)) if finished else None
        updated = finished_at or (created + timedelta(minutes=rng.randint(5, 45)))

        sessions.append((
            session_id,
            user_id,
            recipe_id,
            status,
            current_step,
            servings,
            created,
            updated,
            finished_at,
        ))

    saved_items = []
    for user_id in user_ids:
        for n in range(3):
            recipe_id = rng.choice(recipe_ids)
            saved_id = str(uuid.uuid5(NAMESPACE, f"saved:{user_id}:{n}:{recipe_id}"))
            created = now - timedelta(days=rng.randint(0, 90))

            payload = json.dumps(
                {
                    "recipe_id": recipe_id,
                    "title": recipe_title.get(recipe_id, "Recipe"),
                },
                ensure_ascii=False,
            )

            saved_items.append((
                saved_id,
                user_id,
                "recipe",
                recipe_title.get(recipe_id, "Saved recipe")[:150],
                payload,
                None,
                rng.choice(slots),
                None,
                None,
                created,
                created,
            ))

    return {
        "users": users,
        "preferences": preferences,
        "goals": goals,
        "likes": likes,
        "plans": plans,
        "lists": shopping_lists,
        "list_items": list_items,
        "origins": origins,
        "sessions": sessions,
        "saved_items": saved_items,
    }


def load_all(conn, transformed, mock):
    cursor = conn.cursor()

    verify_target_schema(cursor)
    clear_target(cursor)

    insert_many(cursor, """
        INSERT INTO nutrition.ingredients
        (id, name, base_unit, category, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, transformed["ingredients"])

    insert_many(cursor, """
        INSERT INTO menu.recipes
        (id, title, canonical_name, description, cover_photo_url, servings,
         calories, protein_g, carbs_g, fat_g, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, transformed["recipes"])

    insert_many(cursor, """
        INSERT INTO menu.recipe_categories
        (id, parent_id, name, name_en, slug, sort_order, is_dynamic,
         cover_photo_url, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, transformed["categories"])

    for parent_id, category_id in transformed["category_parent_links"]:
        cursor.execute("""
            UPDATE menu.recipe_categories
            SET parent_id = ?
            WHERE id = ?
              AND EXISTS (
                  SELECT 1
                  FROM menu.recipe_categories
                  WHERE id = ?
              )
        """, parent_id, category_id, parent_id)

    insert_many(cursor, """
        INSERT INTO menu.recipe_category_assignments
        (recipe_id, category_id, created_at)
        VALUES (?, ?, ?)
    """, transformed["category_assignments"])

    insert_many(cursor, """
        INSERT INTO menu.recipe_sections
        (id, recipe_id, title, title_en, description, description_en,
         section_order, source_section_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, transformed["sections"])

    for source_section_id, section_id in transformed["section_source_links"]:
        cursor.execute("""
            UPDATE menu.recipe_sections
            SET source_section_id = ?
            WHERE id = ?
              AND EXISTS (
                  SELECT 1
                  FROM menu.recipe_sections
                  WHERE id = ?
              )
        """, source_section_id, section_id, source_section_id)

    insert_many(cursor, """
        INSERT INTO menu.recipe_composition_ingredients
        (id, recipe_id, ingredient_id, quantity, unit)
        VALUES (?, ?, ?, ?, ?)
    """, transformed["rci"])

    insert_many(cursor, """
        INSERT INTO menu.recipe_steps
        (id, recipe_id, step_number, instruction, section_id)
        VALUES (?, ?, ?, ?, ?)
    """, transformed["steps"])

    insert_many(cursor, """
        INSERT INTO menu.recipe_step_ingredients
        (id, step_id, rci_id, fraction_ratio, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, transformed["rsi"])

    insert_many(cursor, """
        INSERT INTO menu.recipe_time_estimates
        (recipe_id, total_minutes_avg, active_minutes_avg,
         passive_minutes_avg, optional_passive_minutes, last_updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, transformed["times"])

    insert_many(cursor, """
        INSERT INTO pantry.tool_catalog
        (id, code, name_es, name_en, local_name_pe, category,
         criticality_index, kit_min_level, pe_prevalence, is_traditional,
         notes, icon_path, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, transformed["tools"])

    insert_many(cursor, """
        INSERT INTO users.profiles
        (id, username, full_name, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """, mock["users"])

    insert_many(cursor, """
        INSERT INTO users.user_preferences
        (user_id, result_type, available_budget_min_pen, available_budget_max_pen,
         available_time_min, available_time_max, cooking_for, personal_goal,
         currency, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, mock["preferences"])

    insert_many(cursor, """
        INSERT INTO users.user_nutrition_goals
        (user_id, is_active, goal_type, current_weight_kg, target_weight_kg,
         height_cm, age_years, sex, activity_level, final_target_kcal,
         protein_g, carbs_g, fat_g, fiber_g, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, mock["goals"])

    insert_many(cursor, """
        INSERT INTO social.recipe_likes
        (user_id, recipe_id, created_at)
        VALUES (?, ?, ?)
    """, mock["likes"])

    insert_many(cursor, """
        INSERT INTO cookplan.plan_entries
        (id, user_id, recipe_id, day_date, slot_key, servings, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, mock["plans"])

    insert_many(cursor, """
        INSERT INTO cooklist.lists
        (id, user_id, title, start_date, end_date, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, mock["lists"])

    insert_many(cursor, """
        INSERT INTO cooklist.list_items
        (id, list_id, ingredient_id, quantity, unit, is_checked)
        VALUES (?, ?, ?, ?, ?, ?)
    """, mock["list_items"])

    insert_many(cursor, """
        INSERT INTO cooklist.list_item_origins
        (list_item_id, plan_entry_id)
        VALUES (?, ?)
    """, mock["origins"])

    insert_many(cursor, """
        INSERT INTO home.cookmode_sessions
        (id, user_id, recipe_id, status, current_step, servings,
         created_at, updated_at, finished_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, mock["sessions"])

    insert_many(cursor, """
        INSERT INTO cookplan.saved_items
        (id, user_id, item_type, title, content_snapshot, nutrition_snapshot,
         origin_meal_moment, archived_at, deleted_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, mock["saved_items"])

    # El commit se realiza solo después de validar todas las cargas.


def load_dimension(cursor, meta, table, key_column, columns, query):
    """Inserta con clave generada automáticamente o asigna un entero si no es IDENTITY."""
    if key_column not in meta[table]:
        raise RuntimeError(f"Clave desconocida en {table}: {key_column}")
    columns_sql = ", ".join(f"[{c}]" for c in columns)
    names = ", ".join(f"src.[{c}]" for c in columns)
    if meta[table][key_column][1]:
        cursor.execute(f"INSERT INTO {table} ({columns_sql}) SELECT {names} FROM ({query}) src")
    else:
        cursor.execute(
            f"INSERT INTO {table} ([{key_column}], {columns_sql}) "
            f"SELECT CAST(ROW_NUMBER() OVER (ORDER BY src.[_sort]) AS INT), {names} "
            f"FROM ({query}) src"
        )


def load_fact(cursor, meta, table, key_column, columns, query):
    load_dimension(cursor, meta, table, key_column, columns, query)


def load_warehouse(conn, meta):
    c = conn.cursor()
    # Las dimensiones se construyen con las mismas entidades del conjunto operacional.
    calendar_rows = []
    for offset in range(DAYS_TO_PLAN):
        day = START_DATE + timedelta(days=offset)
        month = day.month
        calendar_rows.append((
            int(day.strftime("%Y%m%d")), day, day.day, month,
            calendar.month_name[month], (month - 1) // 3 + 1,
            1 if month <= 6 else 2, day.year, day.isocalendar().week,
        ))
    insert_many(c, """
        INSERT INTO dbo.dim_date
        (date_key,full_date,day_number,month_number,month_name,quarter,semester,year,week_of_year)
        VALUES(?,?,?,?,?,?,?,?,?)
    """, calendar_rows)

    load_dimension(c, meta, "dbo.dim_user", "user_key", [
        "user_id", "username", "result_type", "cooking_for", "personal_goal",
        "nutrition_goal_type", "activity_level", "available_time_min", "available_time_max",
        "target_kcal", "target_protein_g", "target_carbs_g", "target_fat_g",
    ], """
        SELECT CONVERT(VARCHAR(36), u.id) AS user_id, u.username,
               pref.result_type, pref.cooking_for, pref.personal_goal,
               ng.goal_type AS nutrition_goal_type, ng.activity_level,
               pref.available_time_min, pref.available_time_max,
               ng.final_target_kcal AS target_kcal,
               ng.protein_g AS target_protein_g, ng.carbs_g AS target_carbs_g,
               ng.fat_g AS target_fat_g,
               CONVERT(VARCHAR(36), u.id) AS _sort
        FROM users.profiles u
        JOIN users.user_preferences pref ON pref.user_id = u.id
        JOIN users.user_nutrition_goals ng ON ng.user_id = u.id
    """)
    load_dimension(c, meta, "dbo.dim_recipe", "recipe_key", [
        "recipe_id", "title", "canonical_name", "base_servings"
    ], """
        SELECT CONVERT(VARCHAR(36),id) AS recipe_id,title,canonical_name,
               CAST(servings AS DECIMAL(10,2)) AS base_servings,
               CONVERT(VARCHAR(36),id) AS _sort
        FROM menu.recipes
    """)
    load_dimension(c, meta, "dbo.dim_category", "category_key", [
        "category_id", "category_name", "parent_category"
    ], """
        SELECT CONVERT(VARCHAR(36),cat.id) AS category_id,
               cat.name AS category_name, parent.name AS parent_category,
               CONVERT(VARCHAR(36),cat.id) AS _sort
        FROM menu.recipe_categories cat
        LEFT JOIN menu.recipe_categories parent ON parent.id=cat.parent_id
        UNION ALL
        SELECT '{UNCATEGORIZED_CATEGORY_ID}' AS category_id,
               N'Sin categoría' AS category_name,
               CAST(NULL AS NVARCHAR(150)) AS parent_category,
               '{UNCATEGORIZED_CATEGORY_ID}' AS _sort
    """.replace('{UNCATEGORIZED_CATEGORY_ID}', UNCATEGORIZED_CATEGORY_ID))
    load_dimension(c, meta, "dbo.dim_meal_moment", "meal_moment_key", [
        "slot_key", "meal_moment_name", "sort_order"
    ], """
        SELECT v.slot_key, v.meal_moment_name, v.sort_order,
               v.sort_order AS _sort
        FROM (VALUES
            ('breakfast','Desayuno',1),
            ('morning_snack','Media mañana',2),
            ('lunch','Almuerzo',3),
            ('afternoon_snack','Merienda',4),
            ('dinner','Cena',5),
            ('late_night','Noche',6)
        ) v(slot_key,meal_moment_name,sort_order)
    """)
    load_dimension(c, meta, "dbo.dim_ingredient", "ingredient_key", [
        "ingredient_id", "ingredient_name", "ingredient_category", "base_unit"
    ], """
        SELECT CONVERT(VARCHAR(36),id) AS ingredient_id,
               name AS ingredient_name,category AS ingredient_category,
               base_unit,CONVERT(VARCHAR(36),id) AS _sort
        FROM nutrition.ingredients
    """)
    load_dimension(c, meta, "dbo.dim_shopping_list", "shopping_list_key", [
        "list_id", "title", "planning_days"
    ], """
        SELECT CONVERT(VARCHAR(36),id) AS list_id,
               COALESCE(title,'Lista de compra') AS title,
               DATEDIFF(DAY,start_date,end_date)+1 AS planning_days,
               CONVERT(VARCHAR(36),id) AS _sort
        FROM cooklist.lists
    """)
    load_dimension(c, meta, "dbo.dim_purchase_status", "purchase_status_key", [
        "status_code", "status_name"
    ], """
        SELECT v.status_code,v.status_name,v.sort_order AS _sort
        FROM (VALUES ('unchecked','Pendiente',1),('checked','Adquirido',2))
             v(status_code,status_name,sort_order)
    """)
    load_dimension(c, meta, "dbo.dim_session_status", "session_status_key", [
        "status_code", "status_name", "is_completed"
    ], """
        SELECT v.status_code,v.status_name,v.is_completed,v.sort_order AS _sort
        FROM (VALUES ('active','Activa',0,1),('paused','Pausada',0,2),
                     ('finished','Finalizada',1,3),('abandoned','Abandonada',0,4),
                     ('expired','Expirada',0,5))
             v(status_code,status_name,is_completed,sort_order)
    """)

    # Una receta sin categoría conserva su planificación/sesión en la categoría «Sin categoría».
    # Si tiene varias, se elige una categoría determinista para este modelo de cinco dimensiones.
    category_join = f"""
        OUTER APPLY (
            SELECT TOP (1) ca.category_id
            FROM menu.recipe_category_assignments ca
            WHERE ca.recipe_id = r.id ORDER BY ca.category_id
        ) chosen
        JOIN dbo.dim_category cat
          ON cat.category_id = COALESCE(CONVERT(VARCHAR(36),chosen.category_id),
                                        '{UNCATEGORIZED_CATEGORY_ID}')
    """
    load_fact(c, meta, "dbo.fact_planning", "planning_fact_key", [
        "date_key", "user_key", "recipe_key", "category_key", "meal_moment_key",
        "plan_entry_id", "planned_servings", "planned_calories", "planned_protein_g",
        "planned_carbs_g", "planned_fat_g", "estimated_minutes", "plan_count",
    ], f"""
        SELECT d.date_key,u.user_key,dr.recipe_key,cat.category_key,m.meal_moment_key,
               CONVERT(VARCHAR(36),p.id) AS plan_entry_id,
               CAST(p.servings AS DECIMAL(10,2)) AS planned_servings,
               CAST(p.servings*r.calories AS DECIMAL(10,2)) AS planned_calories,
               CAST(p.servings*r.protein_g AS DECIMAL(10,2)) AS planned_protein_g,
               CAST(p.servings*r.carbs_g AS DECIMAL(10,2)) AS planned_carbs_g,
               CAST(p.servings*r.fat_g AS DECIMAL(10,2)) AS planned_fat_g,
               CAST(est.total_minutes_avg AS DECIMAL(10,2)) AS estimated_minutes,
               1 AS plan_count,CONVERT(VARCHAR(36),p.id) AS _sort
        FROM cookplan.plan_entries p
        JOIN menu.recipes r ON r.id=p.recipe_id
        JOIN dbo.dim_date d ON d.full_date=p.day_date
        JOIN dbo.dim_user u ON u.user_id=CONVERT(VARCHAR(36),p.user_id)
        JOIN dbo.dim_recipe dr ON dr.recipe_id=CONVERT(VARCHAR(36),p.recipe_id)
        JOIN dbo.dim_meal_moment m ON m.slot_key=p.slot_key
        LEFT JOIN menu.recipe_time_estimates est ON est.recipe_id=p.recipe_id
        {category_join}
    """)
    load_fact(c, meta, "dbo.fact_cooking_session", "cooking_fact_key", [
        "date_key", "user_key", "recipe_key", "category_key", "session_status_key",
        "session_id", "servings", "last_step_reached", "actual_duration_min",
        "estimated_duration_min", "duration_variance_min", "session_count", "completed_count",
    ], f"""
        SELECT d.date_key,u.user_key,dr.recipe_key,cat.category_key,ss.session_status_key,
               CONVERT(VARCHAR(36),s.id) AS session_id,
               CAST(s.servings AS DECIMAL(10,2)) AS servings,
               s.current_step AS last_step_reached,
               CAST(CASE WHEN s.status='finished'
                 THEN DATEDIFF(SECOND,s.created_at,s.finished_at)/60.0 END
                 AS DECIMAL(10,2)) AS actual_duration_min,
               CAST(est.total_minutes_avg AS DECIMAL(10,2)) AS estimated_duration_min,
               CAST(CASE WHEN s.status='finished'
                 THEN DATEDIFF(SECOND,s.created_at,s.finished_at)/60.0 - est.total_minutes_avg
                 END AS DECIMAL(10,2)) AS duration_variance_min,
               1 AS session_count,CASE WHEN s.status='finished' THEN 1 ELSE 0 END AS completed_count,
               CONVERT(VARCHAR(36),s.id) AS _sort
        FROM home.cookmode_sessions s
        JOIN menu.recipes r ON r.id=s.recipe_id
        JOIN dbo.dim_date d ON d.full_date=CONVERT(DATE,s.created_at)
        JOIN dbo.dim_user u ON u.user_id=CONVERT(VARCHAR(36),s.user_id)
        JOIN dbo.dim_recipe dr ON dr.recipe_id=CONVERT(VARCHAR(36),s.recipe_id)
        JOIN dbo.dim_session_status ss ON ss.status_code=s.status
        LEFT JOIN menu.recipe_time_estimates est ON est.recipe_id=s.recipe_id
        {category_join}
    """)
    load_fact(c, meta, "dbo.fact_shopping_item", "shopping_fact_key", [
        "date_key", "user_key", "ingredient_key", "shopping_list_key", "purchase_status_key",
        "list_item_id", "required_quantity", "item_count", "purchased_item_count",
    ], """
        SELECT d.date_key,u.user_key,di.ingredient_key,dl.shopping_list_key,ps.purchase_status_key,
               CONVERT(VARCHAR(36),li.id) AS list_item_id,
               CAST(li.quantity AS DECIMAL(10,4)) AS required_quantity,
               1 AS item_count,CAST(li.is_checked AS INT) AS purchased_item_count,
               CONVERT(VARCHAR(36),li.id) AS _sort
        FROM cooklist.list_items li
        JOIN cooklist.lists l ON l.id=li.list_id
        JOIN dbo.dim_date d ON d.full_date=CONVERT(DATE,l.created_at)
        JOIN dbo.dim_user u ON u.user_id=CONVERT(VARCHAR(36),l.user_id)
        JOIN dbo.dim_ingredient di ON di.ingredient_id=CONVERT(VARCHAR(36),li.ingredient_id)
        JOIN dbo.dim_shopping_list dl ON dl.list_id=CONVERT(VARCHAR(36),l.id)
        JOIN dbo.dim_purchase_status ps
          ON ps.status_code=CASE WHEN li.is_checked=1 THEN 'checked' ELSE 'unchecked' END
    """)


def verify_loaded_data(conn):
    c = conn.cursor()
    errors=[]
    for table in ALL_TABLES:
        c.execute(f"SELECT COUNT_BIG(*) FROM {table}")
        count = c.fetchone()[0]
        if count == 0:
            errors.append(f"Tabla sin registros tras la carga: {table}")
    expected_pairs = [
        ("dbo.fact_planning", "cookplan.plan_entries"),
        ("dbo.fact_shopping_item", "cooklist.list_items"),
        ("dbo.fact_cooking_session", "home.cookmode_sessions"),
        ("dbo.dim_user", "users.profiles"),
        ("dbo.dim_recipe", "menu.recipes"),
        ("dbo.dim_ingredient", "nutrition.ingredients"),
        ("dbo.dim_shopping_list", "cooklist.lists"),
    ]
    for target, source in expected_pairs:
        c.execute(f"SELECT (SELECT COUNT_BIG(*) FROM {target}), (SELECT COUNT_BIG(*) FROM {source})")
        a,b=c.fetchone()
        if a != b:
            errors.append(f"Inconsistencia entre {target} ({a}) y {source} ({b})")
    for name,table,col in [
        ("planificación", "cookplan.plan_entries", "day_date"),
        ("compras", "cooklist.lists", "start_date"),
        ("cocina", "home.cookmode_sessions", "created_at"),
    ]:
        c.execute(f"SELECT MIN(CONVERT(DATE,{col})), MAX(CONVERT(DATE,{col})), COUNT(DISTINCT YEAR({col})*12 + MONTH({col})) FROM {table}")
        first,last,months = c.fetchone()
        if first != START_DATE or (name != "compras" and last != END_DATE) or months < 7:
            # Siete meses calendario tocados: 23/03 hasta 23/09 = seis meses completos.
            errors.append(f"Periodo {name} inválido: {first} a {last}, {months} meses calendario")
    c.execute("""
        SELECT COUNT_BIG(*)
        FROM cooklist.list_item_origins o
        JOIN cooklist.list_items i ON i.id=o.list_item_id
        JOIN cooklist.lists l ON l.id=i.list_id
        JOIN cookplan.plan_entries p ON p.id=o.plan_entry_id
        WHERE l.user_id <> p.user_id OR p.day_date < l.start_date OR p.day_date > l.end_date
    """)
    invalid=c.fetchone()[0]
    if invalid:
        errors.append(f"Hay {invalid} orígenes de compra incoherentes respecto al usuario o fechas")
    if errors:
        raise RuntimeError("Validación posterior fallida; se revertirá TODO:\n - " + "\n - ".join(errors))


def read_count(conn, table):
    c = conn.cursor()
    c.execute(f"SELECT COUNT_BIG(*) FROM {table}")
    return c.fetchone()[0]


def collect_report(conn, dump_path, source_data):
    cursor = conn.cursor()
    counts = {}

    for table in TARGET_TABLES:
        cursor.execute(f"SELECT COUNT_BIG(*) FROM {table};")
        counts[table] = int(cursor.fetchone()[0])

    source_counts = {
        table: len(source_data.get(table, []))
        for table in sorted(SOURCE_TABLES)
    }

    report = {
        "completed_at": datetime.now().isoformat(timespec="seconds"),
        "source_dump": str(dump_path),
        "sql_server": SQL_SERVER,
        "database": SQL_DATABASE,
        "random_seed": RANDOM_SEED,
        "mock_users": MOCK_USERS,
        "source_counts": source_counts,
        "target_counts": counts,
        "warehouse_counts": {table: int(read_count(conn, table)) for table in DWH_TABLES},
    }

    return report


def main():
    parser = argparse.ArgumentParser(description="CookPilot: limpieza y carga atómica de 6 meses")
    parser.add_argument("--confirm-reset", action="store_true",
                        help="Confirma la sustitución transaccional de datos de las 32 tablas")
    parser.add_argument("--dump", type=Path,
                        help="Ruta opcional al dump de PostgreSQL; por defecto se busca en Descargas")
    args = parser.parse_args()
    if not args.confirm_reset:
        parser.error("Es obligatorio --confirm-reset. No se modificó ningún dato.")

    print("CookPilot - Carga atómica de 6 meses")
    print("=================================")
    dump_path = args.dump or find_dump()
    if not dump_path.is_file():
        raise FileNotFoundError(f"No existe el dump: {dump_path}")
    print(f"Fuente: {dump_path}")
    print(f"Destino: {SQL_SERVER} / {SQL_DATABASE}")
    print(f"Periodo simulado: {START_DATE} a {END_DATE} ({DAYS_TO_PLAN} días)")
    print("Se conservarán todas las tablas y se sustituirán sus registros solo si TODO valida.")

    print("\n1/7 Leyendo PostgreSQL...")
    source_data = parse_copy_dump(dump_path)
    print("2/7 Transformando catálogo...")
    transformed = transform_source(source_data)
    print("3/7 Generando seis meses de actividad...")
    mock = build_mock_data(transformed)
    print(f"  Usuarios: {len(mock['users'])}; planes: {len(mock['plans'])}; "
          f"listas: {len(mock['lists'])}; sesiones: {len(mock['sessions'])}")
    conn = connect_sql_server()
    stage = "validando tablas, columnas, claves y relaciones"
    try:
        conn.execute("SET XACT_ABORT ON")
        print("4/7 Verificando esquema físico y relaciones...")
        meta = verify_full_schema(conn.cursor())
        stage = "limpiando y cargando datos operacionales"
        print("5/7 Limpiando tablas y cargando datos operacionales...")
        load_all(conn, transformed, mock)
        stage = "construyendo almacén dimensional"
        print("6/7 Cargando dimensiones y hechos de BI...")
        load_warehouse(conn, meta)
        stage = "validando recuentos, periodos y coherencia"
        print("7/7 Validación final dentro de la misma transacción...")
        verify_loaded_data(conn)
        report = collect_report(conn, dump_path, source_data)
        conn.commit()
        print("\nCOMMIT: carga completa y validada.")
    except Exception as exc:
        try:
            conn.rollback()
            print("\nROLLBACK EJECUTADO: se conservaron los datos anteriores.", file=sys.stderr)
        except Exception as rb_exc:
            print(f"\nNo se pudo confirmar rollback: {rb_exc}", file=sys.stderr)
        print(f"Etapa fallida: {stage}", file=sys.stderr)
        print(f"Error SQL/Python: {exc}", file=sys.stderr)
        raise
    finally:
        conn.close()

    report["simulation_period"] = {
        "start": START_DATE.isoformat(), "end": END_DATE.isoformat(),
        "days": DAYS_TO_PLAN, "months": 6,
    }
    # Guardar el reporte DESPUÉS del commit: una falla en el reporte no revierte una carga ya confirmada.
    try:
        REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Reporte: {REPORT_PATH}")
    except OSError as err:
        print(f"Aviso: carga confirmada, pero no se pudo guardar el reporte: {err}")
    print("\nDatos operacionales cargados:")
    for table, count in report["target_counts"].items():
        print(f"{table:<42} {count:>10,}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
