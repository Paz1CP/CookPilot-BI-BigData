# -*- coding: utf-8 -*-
r"""Análisis Big Data de CookPilot: recorrido, comportamiento y preparación.

Lee SQL Server por JDBC en modo de solo consulta. No modifica ninguna tabla.

Entorno comprobado por el usuario: Spark 4.2.0, Windows 11, SQL Server 2022,
JDBC Microsoft 13.6 con autenticación integrada de Windows.

Ejemplo de PowerShell (una sola línea):
$env:JAVA_TOOL_OPTIONS="-Djava.library.path=C:\spark\jdbc"; $env:Path="C:\spark\jdbc;$env:Path"; spark-submit --driver-library-path "C:\spark\jdbc" "C:\spark\cookpilot_bigdata_spark.py"

Salidas: CSV agregados, JSON de ejecución y PNG si matplotlib está disponible.
No se exportan nombres de usuario, correos ni registros operacionales individuales.
"""

import argparse
import csv
import json
import os
import platform
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

VERSION = "1.0.0"
FUENTES = {
    "planning": "dbo.fact_planning",
    "shopping": "dbo.fact_shopping_item",
    "cooking": "dbo.fact_cooking_session",
    "date": "dbo.dim_date",
    "category": "dbo.dim_category",
    "session_status": "dbo.dim_session_status",
    "users": "dbo.dim_user",
    "origins": "cooklist.list_item_origins",
}


def argumentos():
    p = argparse.ArgumentParser(description="Tres análisis de CookPilot con PySpark")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=1433)
    p.add_argument("--database", default="CookPilot")
    p.add_argument("--output", default=str(Path(__file__).resolve().parent / "resultados_bigdata"))
    p.add_argument("--cores", type=int, default=2, help="Núcleos locales de Spark")
    p.add_argument("--partitions", type=int, default=4, help="Particiones de transformación")
    p.add_argument("--window-days", type=int, default=7, help="Ventana de asociación no causal")
    p.add_argument("--sin-graficos", action="store_true")
    a = p.parse_args()
    if not 1 <= a.cores <= 64 or not 1 <= a.partitions <= 128:
        p.error("--cores y --partitions deben ser positivos y estar dentro de los límites")
    if not 0 <= a.window_days <= 30:
        p.error("--window-days debe estar entre 0 y 30")
    if not 1 <= a.port <= 65535:
        p.error("Puerto no válido")
    return a


def iniciar_spark(args):
    spark = (
        SparkSession.builder.appName("CookPilot-BigData-A-B-C")
        .master(f"local[{args.cores}]")
        .config("spark.sql.shuffle.partitions", str(args.partitions))
        .config("spark.sql.session.timeZone", "America/Lima")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def memoria_driver_jvm(spark):
    runtime = spark._jvm.java.lang.Runtime.getRuntime()
    return {
        "heap_utilizado_mib": round((runtime.totalMemory() - runtime.freeMemory()) / (1024 ** 2), 2),
        "heap_maximo_mib": round(runtime.maxMemory() / (1024 ** 2), 2),
    }


def leer_jdbc(spark, jdbc_url, tabla):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", tabla)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", "10000")
        .load()
    )


def requerir(df, tabla, columnas):
    faltantes = sorted(set(columnas) - set(df.columns))
    if faltantes:
        raise ValueError(f"La tabla {tabla} no contiene estas columnas: {faltantes}")


def fecha_evento(df, dim_fecha, nombre_fecha="fecha"):
    return (
        df.join(dim_fecha.select("date_key", F.col("full_date").alias(nombre_fecha)), "date_key", "left")
        .withColumn("mes", F.date_format(F.col(nombre_fecha), "yyyy-MM"))
    )


def guion_id(nombre):
    return F.lower(F.trim(F.col(nombre).cast("string")))


def normalizar_id(df, nombre):
    return df.withColumn(nombre, guion_id(nombre))


def metrica_calidad(df, pk, tabla):
    n, no_pk, distintos = (
        df.agg(
            F.count(F.lit(1)).alias("n"),
            F.sum(F.when(F.col(pk).isNull(), 1).otherwise(0)).alias("nulos"),
            F.countDistinct(pk).alias("distintos"),
        ).first()
    )
    return {"tabla": tabla, "filas": int(n), "pk_nulas": int(no_pk), "pk_duplicadas": int(n - no_pk - distintos)}


def exportar(df, carpeta, nombre, max_filas=10000):
    filas = df.limit(max_filas + 1).collect()
    if len(filas) > max_filas:
        raise ValueError(f"Salida {nombre}: supera {max_filas} filas; ajuste el nivel de agrupación")
    destino = carpeta / f"{nombre}.csv"
    with destino.open("w", newline="", encoding="utf-8-sig") as f:
        escritor = csv.writer(f)
        escritor.writerow(df.columns)
        for fila in filas:
            escritor.writerow([v.isoformat() if hasattr(v, "isoformat") else v for v in fila])
    print(f"  {destino.name}: {len(filas)} filas")
    return {"archivo": str(destino), "filas": len(filas)}


def porcentaje(parte, total):
    return round(100 * parte / total, 2) if total else None


def analisis_a(plan, compras, sesiones, origenes, carpeta, ventana):
    """Plan→ítems: origen verificable. Plan→sesión: asociación temporal estimada."""
    print("\n[A] Recorrido planificación -> compras -> cocina")
    inicio = time.perf_counter()

    p = (
        plan.select("plan_entry_id", "user_key", "recipe_key", "fecha_plan", "mes")
        .filter(F.col("plan_entry_id").isNotNull() & F.col("fecha_plan").isNotNull())
        .dropDuplicates(["plan_entry_id"])
    )
    items = (
        compras.select("list_item_id", "user_key", "shopping_list_key", "purchased_item_count")
        .filter(F.col("list_item_id").isNotNull())
        .dropDuplicates(["list_item_id"])
    )
    links = (
        origenes.select("plan_entry_id", "list_item_id")
        .filter(F.col("plan_entry_id").isNotNull() & F.col("list_item_id").isNotNull())
        .dropDuplicates()
    )

    unidos = (
        links.join(
            p.select("plan_entry_id", F.col("user_key").alias("usuario_plan")),
            "plan_entry_id", "inner"
        ).join(
            items.select("list_item_id", F.col("user_key").alias("usuario_compra"), "purchased_item_count"),
            "list_item_id", "inner"
        )
    )
    usuarios_incoherentes = unidos.filter(F.col("usuario_plan") != F.col("usuario_compra")).count()
    coincidencias_identidad = unidos.count()
    if coincidencias_identidad == 0 and links.count() > 0:
        raise ValueError("No hay coincidencias entre el origen de compra y las claves del almacén: revise plan_entry_id y list_item_id")
    validos = unidos.filter(F.col("usuario_plan") == F.col("usuario_compra"))
    con_items = validos.groupBy("plan_entry_id").agg(
        F.max(F.when(F.col("purchased_item_count") > 0, 1).otherwise(0)).alias("tiene_adquirido"),
        F.countDistinct("list_item_id").alias("items_vinculados"),
    )
    cobertura = (
        p.select("plan_entry_id", "mes")
        .join(con_items, "plan_entry_id", "left")
        .groupBy("mes")
        .agg(
            F.count("plan_entry_id").alias("planificaciones"),
            F.sum(F.when(F.col("items_vinculados") > 0, 1).otherwise(0)).alias("planes_con_items_compra"),
            F.sum(F.when(F.col("tiene_adquirido") == 1, 1).otherwise(0)).alias("planes_con_item_adquirido"),
        )
    )

    p_cocina = p.filter(F.col("user_key").isNotNull() & F.col("recipe_key").isNotNull())
    s_cocina = (
        sesiones.select("session_id", "user_key", "recipe_key", "fecha_sesion")
        .filter(F.col("session_id").isNotNull() & F.col("fecha_sesion").isNotNull())
        .dropDuplicates(["session_id"])
    )
    condicion = (
        (F.col("s.user_key") == F.col("p.user_key"))
        & (F.col("s.recipe_key") == F.col("p.recipe_key"))
        & (F.col("s.fecha_sesion") >= F.col("p.fecha_plan"))
        & (F.col("s.fecha_sesion") <= F.date_add(F.col("p.fecha_plan"), ventana))
    )
    candidatos = (
        s_cocina.alias("s").join(p_cocina.alias("p"), condicion, "inner")
        .select(
            F.col("s.session_id").alias("session_id"),
            F.col("p.plan_entry_id").alias("plan_entry_id"),
            F.col("p.mes").alias("mes"),
            F.col("p.fecha_plan").alias("fecha_plan"),
        )
    )
    orden = Window.partitionBy("session_id").orderBy(F.col("fecha_plan").desc(), F.col("plan_entry_id"))
    asociaciones = candidatos.withColumn("orden", F.row_number().over(orden)).filter("orden = 1").drop("orden")
    ses_por_mes = asociaciones.groupBy("mes").agg(
        F.countDistinct("session_id").alias("sesiones_asociadas_estimadas"),
        F.countDistinct("plan_entry_id").alias("planes_con_sesion_asociada_estimada"),
    )
    resultado = (
        cobertura.join(ses_por_mes, "mes", "left")
        .fillna({"sesiones_asociadas_estimadas": 0, "planes_con_sesion_asociada_estimada": 0})
        .withColumn("planes_con_items_pct", F.round(100 * F.col("planes_con_items_compra") / F.col("planificaciones"), 2))
        .withColumn("planes_con_item_adquirido_pct", F.round(100 * F.col("planes_con_item_adquirido") / F.col("planificaciones"), 2))
        .withColumn("planes_con_sesion_estimada_pct", F.round(100 * F.col("planes_con_sesion_asociada_estimada") / F.col("planificaciones"), 2))
        .orderBy("mes")
    )
    salida = exportar(resultado, carpeta, "A_recorrido_mensual")
    resumen = resultado.agg(
        F.sum("planificaciones").alias("planificaciones"),
        F.sum("planes_con_items_compra").alias("planes_con_items_compra"),
        F.sum("planes_con_item_adquirido").alias("planes_con_item_adquirido"),
        F.sum("sesiones_asociadas_estimadas").alias("sesiones_asociadas_estimadas"),
        F.sum("planes_con_sesion_asociada_estimada").alias("planes_con_sesion_asociada_estimada"),
    ).first().asDict()
    resumen = {k: int(v or 0) for k, v in resumen.items()}
    resumen["planes_con_items_pct"] = porcentaje(resumen["planes_con_items_compra"], resumen["planificaciones"])
    resumen["planes_con_sesion_estimada_pct"] = porcentaje(resumen["planes_con_sesion_asociada_estimada"], resumen["planificaciones"])
    resumen["relaciones_origen_con_usuario_distinto"] = usuarios_incoherentes
    resumen["relaciones_origen_presentes_en_ambos_hechos"] = coincidencias_identidad
    resumen["ventana_asociacion_dias"] = ventana
    resumen["criterio_origen"] = "cooklist.list_item_origins: plan_entry_id + list_item_id (verificado)"
    resumen["criterio_cocina"] = "Coincidencia usuario/receta; sesión entre fecha planificada y N días después; asignación a planificación previa más reciente. No es una conversión causal."
    resumen["segundos"] = round(time.perf_counter() - inicio, 3)
    return resumen, [salida]


def analisis_b(plan, compras, sesiones, dim_fecha, carpeta):
    """Actividad mensual, perfiles agregados y recurrencia al mes siguiente."""
    print("\n[B] Comportamiento y recurrencia de usuarios")
    inicio = time.perf_counter()
    p = plan.groupBy("user_key", "mes").agg(F.countDistinct("plan_entry_id").alias("planificaciones"))
    i = compras.groupBy("user_key", "mes").agg(
        F.countDistinct("list_item_id").alias("items_compra"),
        F.countDistinct("shopping_list_key").alias("listas_compra"),
        F.sum(F.coalesce(F.col("purchased_item_count"), F.lit(0))).alias("items_adquiridos"),
    )
    s = sesiones.groupBy("user_key", "mes").agg(
        F.countDistinct("session_id").alias("sesiones"),
        F.sum(F.col("sesion_finalizada")).alias("sesiones_finalizadas"),
    )
    llaves = (
        p.select("user_key", "mes").unionByName(i.select("user_key", "mes"))
        .unionByName(s.select("user_key", "mes")).distinct()
        .filter(F.col("user_key").isNotNull() & F.col("mes").isNotNull())
    )
    actividad = (
        llaves.join(p, ["user_key", "mes"], "left")
        .join(i, ["user_key", "mes"], "left")
        .join(s, ["user_key", "mes"], "left")
        .fillna(0, subset=["planificaciones", "items_compra", "listas_compra", "items_adquiridos", "sesiones", "sesiones_finalizadas"])
        .withColumn("procesos_activos", (
            (F.col("planificaciones") > 0).cast("int") +
            (F.col("items_compra") > 0).cast("int") +
            (F.col("sesiones") > 0).cast("int")
        ))
    )
    actividad = actividad.withColumn(
        "perfil_mensual",
        F.when(F.col("procesos_activos") == 3, "planifica_compra_cocina")
        .when(F.col("procesos_activos") == 2, "dos_procesos")
        .otherwise("un_proceso"),
    )
    mensual = actividad.groupBy("mes").agg(
        F.countDistinct("user_key").alias("usuarios_activos"),
        F.sum(F.when(F.col("planificaciones") > 0, 1).otherwise(0)).alias("usuarios_planifican"),
        F.sum(F.when(F.col("items_compra") > 0, 1).otherwise(0)).alias("usuarios_compran"),
        F.sum(F.when(F.col("sesiones") > 0, 1).otherwise(0)).alias("usuarios_cocinan"),
        F.sum(F.when(F.col("procesos_activos") == 3, 1).otherwise(0)).alias("usuarios_tres_procesos"),
        F.sum(F.when(F.col("procesos_activos") == 2, 1).otherwise(0)).alias("usuarios_dos_procesos"),
        F.sum(F.when(F.col("procesos_activos") == 1, 1).otherwise(0)).alias("usuarios_un_proceso"),
    ).orderBy("mes")
    usuarios = actividad.groupBy("user_key").agg(
        F.countDistinct("mes").alias("meses_activos"),
        F.sum("planificaciones").alias("planificaciones"),
        F.sum("listas_compra").alias("listas_compra_mes_sumadas"),
        F.sum("items_compra").alias("items_compra"),
        F.sum("sesiones").alias("sesiones"),
        F.sum("sesiones_finalizadas").alias("sesiones_finalizadas"),
    ).withColumn(
        "procesos_utilizados",
        (F.col("planificaciones") > 0).cast("int") +
        (F.col("items_compra") > 0).cast("int") +
        (F.col("sesiones") > 0).cast("int"),
    ).withColumn(
        "perfil_periodo", F.when(F.col("procesos_utilizados") == 3, "tres_procesos")
        .when(F.col("procesos_utilizados") == 2, "dos_procesos").otherwise("un_proceso"),
    ).orderBy("user_key")

    planes_activos = actividad.filter(F.col("planificaciones") > 0).select("user_key", "mes")
    siguiente = planes_activos.select(
        "user_key",
        F.date_format(F.add_months(F.to_date(F.concat(F.col("mes"), F.lit("-01"))), -1), "yyyy-MM").alias("mes"),
    ).withColumn("regreso", F.lit(1))
    ultima_fecha = dim_fecha.agg(F.max("full_date").alias("ultima")).first()["ultima"]
    if ultima_fecha is None:
        raise ValueError("dim_date.full_date no contiene fechas")
    ultimo_mes = ultima_fecha.strftime("%Y-%m")
    retencion = (
        planes_activos.join(siguiente, ["user_key", "mes"], "left")
        .filter(F.col("mes") < F.lit(ultimo_mes))
        .groupBy("mes")
        .agg(F.count("user_key").alias("usuarios_planificadores"),
             F.sum(F.coalesce(F.col("regreso"), F.lit(0))).alias("usuarios_regresan_mes_siguiente"))
        .withColumn("recurrencia_pct", F.round(100 * F.col("usuarios_regresan_mes_siguiente") / F.col("usuarios_planificadores"), 2))
        .orderBy("mes")
    )
    salidas = [
        exportar(mensual, carpeta, "B_actividad_mensual"),
        exportar(actividad.orderBy("mes", "user_key"), carpeta, "B_usuarios_mensual"),
        exportar(usuarios, carpeta, "B_perfiles_usuarios"),
        exportar(retencion, carpeta, "B_recurrencia_mensual"),
    ]
    resumen = {
        "usuarios_activos": int(usuarios.count()),
        "meses_observados": int(mensual.count()),
        "ultimo_mes_sin_recurrencia_futura": ultimo_mes,
        "nota": "La recurrencia se calcula sobre planificadores de meses consecutivos; el último mes se excluye por falta de datos futuros. Los meses extremos pueden estar incompletos.",
        "segundos": round(time.perf_counter() - inicio, 3),
    }
    return resumen, salidas


def analisis_c(sesiones, carpeta):
    """Tasa sobre estados terminales; duración solo para finalizadas válidas."""
    print("\n[C] Finalización, duración y precisión por categoría")
    inicio = time.perf_counter()
    base = sesiones.withColumn(
        "tiempo_valido", (F.col("sesion_finalizada") == 1)
        & F.col("actual_duration_min").isNotNull()
        & (F.col("actual_duration_min") >= 0)
        & F.col("estimated_duration_min").isNotNull()
        & (F.col("estimated_duration_min") > 0),
    ).withColumn(
        "diferencia_calculada_min", F.col("actual_duration_min") - F.col("estimated_duration_min")
    ).withColumn(
        "dentro_20pct", F.when(F.col("tiempo_valido") & (F.abs(F.col("diferencia_calculada_min")) <= F.col("estimated_duration_min") * 0.20), 1).otherwise(0)
    )
    def resumen_por(grupos):
        return base.groupBy(*grupos).agg(
            F.countDistinct("session_id").alias("sesiones_registradas"),
            F.sum("sesion_terminal").alias("sesiones_terminales"),
            F.sum("sesion_finalizada").alias("sesiones_finalizadas"),
            F.sum(F.when(F.col("tiempo_valido"), 1).otherwise(0)).alias("sesiones_tiempo_valido"),
            F.avg(F.when(F.col("tiempo_valido"), F.col("actual_duration_min"))).alias("duracion_promedio_min"),
            F.avg(F.when(F.col("tiempo_valido"), F.col("diferencia_calculada_min"))).alias("diferencia_promedio_min"),
            F.sum("dentro_20pct").alias("sesiones_dentro_20pct"),
        ).withColumn(
            "finalizacion_pct", F.when(F.col("sesiones_terminales") > 0, F.round(100 * F.col("sesiones_finalizadas") / F.col("sesiones_terminales"), 2))
        ).withColumn(
            "sesiones_dentro_20pct_pct", F.when(F.col("sesiones_tiempo_valido") > 0, F.round(100 * F.col("sesiones_dentro_20pct") / F.col("sesiones_tiempo_valido"), 2))
        )
    mes_categoria = resumen_por(["mes", "categoria"]).orderBy("mes", "categoria")
    categoria = resumen_por(["categoria"]).orderBy(F.desc("sesiones_terminales"), "categoria")
    mes = resumen_por(["mes"]).orderBy("mes")
    salidas = [
        exportar(mes_categoria, carpeta, "C_categoria_mes"),
        exportar(categoria, carpeta, "C_categorias"),
        exportar(mes, carpeta, "C_mensual"),
    ]
    inconsistencias = base.filter(
        F.col("duration_variance_min").isNotNull()
        & F.col("actual_duration_min").isNotNull()
        & F.col("estimated_duration_min").isNotNull()
        & (F.abs(F.col("duration_variance_min") - F.col("diferencia_calculada_min")) > 0.11)
    ).count()
    resumen = {
        "diferencias_almacenadas_distintas_del_calculo": inconsistencias,
        "estados_terminales": ["finished", "abandoned", "expired"],
        "nota": "La duración y su desviación se calculan únicamente para sesiones finalizadas con duración >=0 y tiempo estimado >0. La tasa de finalización excluye los estados activos y pausados.",
        "segundos": round(time.perf_counter() - inicio, 3),
    }
    return resumen, salidas


def generar_graficos(carpeta, sin_graficos=False):
    if sin_graficos:
        return []
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("AVISO: No se encontró matplotlib. Los CSV y JSON se generaron; instale matplotlib para crear los PNG.")
        return []

    fondo, panel, texto = "#080000", "#181516", "#F4F3F0"
    amarillo, azul, verde, gris = "#E9A80E", "#67A6EA", "#4DBAA8", "#A0A0A0"
    plt.rcParams.update({
        "figure.facecolor": fondo, "axes.facecolor": panel, "axes.edgecolor": gris,
        "axes.labelcolor": texto, "text.color": texto, "xtick.color": texto,
        "ytick.color": texto, "font.size": 10, "grid.alpha": 0.15,
        "savefig.facecolor": fondo,
    })
    creados = []

    def leer_csv(nombre):
        with (carpeta / f"{nombre}.csv").open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))

    def num(fila, clave):
        try:
            return float(fila.get(clave) or 0)
        except (TypeError, ValueError):
            return 0.0

    def guardar(nombre):
        plt.tight_layout(pad=2)
        destino = carpeta / f"{nombre}.png"
        plt.savefig(destino, dpi=170, bbox_inches="tight")
        plt.close()
        creados.append(str(destino))
        print(f"  {destino.name}")

    a = leer_csv("A_recorrido_mensual")
    if a:
        fig, ax = plt.subplots(figsize=(11, 5.5))
        m = [r["mes"] for r in a]
        x = range(len(m))
        ax.plot(x, [num(r, "planificaciones") for r in a], marker="o", lw=2.5, color=azul, label="Planificaciones")
        ax.plot(x, [num(r, "planes_con_items_compra") for r in a], marker="s", lw=2, color=amarillo, label="Con ítems de compra verificados")
        ax.plot(x, [num(r, "planes_con_sesion_asociada_estimada") for r in a], marker="^", lw=2, color=verde, label="Con sesión asociada (estimación)")
        ax.set_xticks(list(x), m)
        ax.set_title("Recorrido culinario por mes", loc="left", fontsize=15, fontweight="bold")
        ax.set_ylabel("Entradas de planificación")
        ax.grid(axis="y")
        ax.legend(frameon=False, labelcolor=texto, loc="upper left", fontsize=9)
        guardar("A_recorrido_culinario")

    b = leer_csv("B_actividad_mensual")
    if b:
        fig, ax = plt.subplots(figsize=(11, 5.5))
        meses = [r["mes"] for r in b]
        xs = list(range(len(meses)))
        uno = [num(r, "usuarios_un_proceso") for r in b]
        dos = [num(r, "usuarios_dos_procesos") for r in b]
        tres = [num(r, "usuarios_tres_procesos") for r in b]
        ax.bar(xs, uno, label="Un proceso", color=azul)
        ax.bar(xs, dos, bottom=uno, label="Dos procesos", color=amarillo)
        ax.bar(xs, tres, bottom=[x+y for x, y in zip(uno, dos)], label="Tres procesos", color=verde)
        ax.set_xticks(xs, meses)
        ax.set_ylabel("Usuarios distintos")
        ax.set_title("Participación mensual por cantidad de procesos", loc="left", fontsize=15, fontweight="bold")
        ax.grid(axis="y")
        ax.legend(frameon=False, labelcolor=texto)
        guardar("B_comportamiento_usuarios")

    c = leer_csv("C_categorias")
    c = [r for r in c if num(r, "sesiones_terminales") >= 20 and r.get("finalizacion_pct")]
    c.sort(key=lambda r: num(r, "finalizacion_pct"))
    if c:
        c = c[:12][::-1]
        fig, ax = plt.subplots(figsize=(11, 6.3))
        ax.barh([r["categoria"] for r in c], [num(r, "finalizacion_pct") for r in c], color=azul)
        ax.axvline(65, color=amarillo, lw=2, label="Meta de referencia: 65 %")
        ax.set_xlim(0, 100)
        ax.set_xlabel("Finalización entre estados terminales (%)")
        ax.set_title("Categorías con menor tasa de finalización", loc="left", fontsize=15, fontweight="bold")
        ax.grid(axis="x")
        ax.legend(frameon=False, labelcolor=texto)
        guardar("C_finalizacion_por_categoria")

    r = leer_csv("B_recurrencia_mensual")
    if r:
        fig, ax = plt.subplots(figsize=(10, 5))
        meses = [fila["mes"] for fila in r]
        ax.plot(meses, [num(fila, "recurrencia_pct") for fila in r], marker="o", lw=2.5, color=verde)
        ax.axhline(50, lw=1.8, color=amarillo, label="Referencia: 50 %")
        ax.set_ylim(0, 105)
        ax.set_ylabel("Usuarios que vuelven al mes siguiente (%)")
        ax.set_title("Recurrencia mensual de planificación", loc="left", fontsize=15, fontweight="bold")
        ax.grid(axis="y")
        ax.legend(frameon=False, labelcolor=texto)
        guardar("B_recurrencia")
    return creados


def main():
    args = argumentos()
    inicio_total = time.perf_counter()
    fecha_inicio = datetime.now().astimezone()
    carpeta = Path(args.output).resolve() / fecha_inicio.strftime("ejecucion_%Y%m%d_%H%M%S")
    carpeta.mkdir(parents=True, exist_ok=False)
    reporte = {
        "version_codigo": VERSION,
        "inicio": fecha_inicio.isoformat(),
        "fuente": f"{args.host}:{args.port}/{args.database}",
        "modo": "local; batch JDBC; solo lectura; datos sintéticos",
        "configuracion": {"cores": args.cores, "particiones_transformacion": args.partitions, "particiones_lectura_jdbc": 1, "ventana_asociacion_dias": args.window_days, "jdbc_fetchsize": 10000},
        "tablas": FUENTES,
        "calidad": [],
        "analisis": {},
        "salidas": [],
        "graficos": [],
        "estado": "en_ejecucion",
        "advertencias": [
            "No se acredita procesamiento en un clúster multinodo: Spark opera en una sola computadora.",
            "Los resultados describen datos sintéticos y no comportamiento real de consumidores.",
            "Las planificaciones relacionadas con cocina son asociaciones temporales; no un vínculo de origen comprobado.",
            "No se mide compra monetaria: adquirido equivale a producto marcado en una lista.",
            "La extracción JDBC utiliza una partición por tabla; las particiones configurables corresponden al procesamiento posterior.",
        ],
    }
    spark = None
    try:
        spark = iniciar_spark(args)
        reporte["entorno"] = {
            "spark": spark.version,
            "python": platform.python_version(),
            "sistema": platform.platform(),
            "java": str(spark._jvm.java.lang.System.getProperty("java.version")),
        }
        url = (
            f"jdbc:sqlserver://{args.host}:{args.port};databaseName={args.database};"
            "encrypt=true;trustServerCertificate=true;integratedSecurity=true;"
            "authenticationScheme=NativeAuthentication"
        )
        reporte["memoria_driver_jvm_inicio"] = memoria_driver_jvm(spark)
        print(f"CookPilot Big Data {VERSION} | Spark {spark.version} | SQL Server {args.host}:{args.port}")
        datos = {}
        for clave, tabla in FUENTES.items():
            try:
                datos[clave] = leer_jdbc(spark, url, tabla)
                print(f"  Fuente disponible: {tabla}")
            except Exception as exc:
                raise RuntimeError(f"No se pudo leer {tabla} por JDBC. Revise tabla, permisos y driver. Detalle: {exc}") from exc

        columnas = {
            "planning": ["plan_entry_id", "user_key", "recipe_key", "date_key"],
            "shopping": ["list_item_id", "user_key", "date_key", "shopping_list_key", "purchased_item_count"],
            "cooking": ["session_id", "user_key", "recipe_key", "date_key", "category_key", "session_status_key", "actual_duration_min", "estimated_duration_min", "duration_variance_min"],
            "date": ["date_key", "full_date"],
            "category": ["category_key", "category_name"],
            "session_status": ["session_status_key", "status_code", "is_completed"],
            "users": ["user_key"],
            "origins": ["list_item_id", "plan_entry_id"],
        }
        for clave, cols in columnas.items():
            requerir(datos[clave], FUENTES[clave], cols)

        rango_fechas = datos["date"].agg(
            F.min("full_date").alias("inicio"),
            F.max("full_date").alias("fin"),
            F.count(F.lit(1)).alias("dias"),
        ).first()
        reporte["periodo_datos"] = {
            "desde": str(rango_fechas["inicio"]),
            "hasta": str(rango_fechas["fin"]),
            "fechas": int(rango_fechas["dias"]),
        }
        dim_fecha = datos["date"].select("date_key", "full_date").dropDuplicates(["date_key"])
        plan = fecha_evento(datos["planning"], dim_fecha, "fecha_plan")
        compras = fecha_evento(datos["shopping"], dim_fecha, "fecha_compra")
        cocina = fecha_evento(datos["cooking"], dim_fecha, "fecha_sesion")
        categoria = datos["category"].select("category_key", F.col("category_name").alias("categoria"))
        estado = datos["session_status"].select("session_status_key", "status_code", "is_completed")
        cocina = cocina.join(categoria, "category_key", "left").join(estado, "session_status_key", "left")
        cocina = cocina.withColumn("categoria", F.coalesce(F.col("categoria"), F.lit("Sin categoría")))
        codigo = F.lower(F.trim(F.col("status_code")))
        cocina = cocina.withColumn("sesion_finalizada", F.when((F.col("is_completed") == True) | codigo.isin("finished", "completed"), 1).otherwise(0))
        cocina = cocina.withColumn("sesion_terminal", F.when(codigo.isin("finished", "completed", "abandoned", "expired"), 1).otherwise(0))

        plan = normalizar_id(plan, "plan_entry_id")
        compras = normalizar_id(compras, "list_item_id")
        origenes = normalizar_id(normalizar_id(datos["origins"], "plan_entry_id"), "list_item_id")
        plan = plan.repartition(args.partitions, "user_key").persist(StorageLevel.MEMORY_AND_DISK)
        compras = compras.repartition(args.partitions, "user_key").persist(StorageLevel.MEMORY_AND_DISK)
        cocina = cocina.repartition(args.partitions, "user_key").persist(StorageLevel.MEMORY_AND_DISK)
        origenes = origenes.repartition(args.partitions, "plan_entry_id").persist(StorageLevel.MEMORY_AND_DISK)

        for clave, df, pk in [
            ("planning", plan, "plan_entry_id"),
            ("shopping", compras, "list_item_id"),
            ("cooking", cocina, "session_id"),
        ]:
            q = metrica_calidad(df, pk, FUENTES[clave])
            reporte["calidad"].append(q)
            if q["pk_nulas"] or q["pk_duplicadas"]:
                raise ValueError(f"Identificadores inválidos en {q['tabla']}: {q}")
            sin_fecha = df.filter(F.col("mes").isNull()).count()
            if sin_fecha:
                raise ValueError(f"{q['tabla']}: {sin_fecha} registros sin fecha en dim_date")
            print(f"  {q['tabla']}: {q['filas']:,} filas; {args.partitions} particiones")
        reporte["calidad"].append({"tabla": FUENTES["origins"], "filas": origenes.count()})
        reporte["calidad"].append({"tabla": FUENTES["users"], "filas": datos["users"].count()})
        estados_vistos = [r.asDict() for r in cocina.groupBy("status_code").count().orderBy("status_code").collect()]
        reporte["estados_sesion"] = estados_vistos
        codigos_esperados = {"active", "paused", "finished", "completed", "abandoned", "expired"}
        codigos_nuevos = [r.get("status_code") for r in estados_vistos if str(r.get("status_code") or "").strip().lower() not in codigos_esperados]
        if codigos_nuevos:
            reporte["advertencias"].append(f"Estados no contemplados en la tasa terminal: {codigos_nuevos}")
        print("  Estados de sesión:", estados_vistos)

        for clave, funcion in [
            ("A", lambda: analisis_a(plan, compras, cocina, origenes, carpeta, args.window_days)),
            ("B", lambda: analisis_b(plan, compras, cocina, dim_fecha, carpeta)),
            ("C", lambda: analisis_c(cocina, carpeta)),
        ]:
            resumen, salidas = funcion()
            reporte["analisis"][clave] = resumen
            reporte["salidas"].extend(salidas)
        reporte["graficos"] = generar_graficos(carpeta, args.sin_graficos)
        reporte["memoria_driver_jvm_fin"] = memoria_driver_jvm(spark)
        reporte["estado"] = "completado"
        print("\nANÁLISIS A, B Y C COMPLETADOS")
    except Exception as exc:
        reporte["estado"] = "fallido"
        reporte["error"] = f"{type(exc).__name__}: {exc}"
        print(f"\nERROR: {exc}", file=sys.stderr)
        traceback.print_exc()
        raise
    finally:
        reporte["fin"] = datetime.now().astimezone().isoformat()
        reporte["segundos_totales"] = round(time.perf_counter() - inicio_total, 3)
        destino = carpeta / "reporte_ejecucion.json"
        with destino.open("w", encoding="utf-8") as f:
            json.dump(reporte, f, ensure_ascii=False, indent=2, default=str)
        print(f"\nResultados en: {carpeta}")
        print(f"Informe de ejecución: {destino}")
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
