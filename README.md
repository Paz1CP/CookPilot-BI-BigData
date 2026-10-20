# CookPilot — Inteligencia de Negocios y Big Data

Proyecto académico de **Base de Datos Avanzadas y Big Data**.

Repositorio: https://github.com/Paz1CP/CookPilot-BI-BigData

## 1. Descripción

Este proyecto implementa una solución integrada de Inteligencia de Negocios (BI) y procesamiento Big Data sobre información de CookPilot, una plataforma de planificación y preparación de recetas.

La solución utiliza un almacén dimensional implementado en **SQL Server 2022**, un informe interactivo desarrollado con **Power BI** y un proceso de análisis mediante **Apache Spark 4.2.0 y PySpark**.

Los datos utilizados incluyen información de catálogo y actividad sintética generada para el estudio, correspondientes al periodo comprendido entre marzo y septiembre de 2026.

### Arquitectura implementada

```text
Datos de catálogo + actividad sintética
                   |
                   v
             Python (ETL)
                   |
                   v
             SQL Server 2022
      Tablas operacionales y Data Warehouse
                   |
          +--------+--------+
          |                 |
          v                 v
       Power BI        Spark + PySpark
          |                 |
          v                 v
   Dashboards OLAP     Análisis A, B y C
                            |
                            v
                     CSV + PNG + JSON
```

El Data Warehouse es relacional y se encuentra dentro de la base `CookPilot`. No se implementó un Data Lake, almacenamiento en AWS S3 ni un clúster distribuido.

## 2. Tecnologías

| Tecnología | Utilización |
|---|---|
| SQL Server 2022 | Base de datos operacional y almacén dimensional |
| Python | Generación, transformación y carga ETL |
| Power BI Desktop | Modelo semántico, indicadores, operaciones OLAP y dashboards |
| Apache Spark 4.2.0 | Procesamiento analítico paralelo local |
| PySpark | Implementación de análisis Big Data |
| JDBC Microsoft 13.6.0 | Conexión entre Spark y SQL Server |
| ODBC para SQL Server | Conexión del proceso ETL |
| Matplotlib | Visualizaciones de los resultados Big Data |
| Git y Git LFS | Versionado y almacenamiento de archivos de base de datos |

El entorno técnico comprobado utilizó Windows 11, Python 3.14.2 y Java 24.0.2.

## 3. Estructura del repositorio

```text
CookPilot-BI-BigData/
│
├── README.md
├── .gitattributes
│
├── database/
│   ├── CookPilot_20261009_093937.bak
│   └── CookPilot_Schema_And_Data.sql
│
├── etl/
│   └── seed_cookpilot_sql.py
│
├── bi/
│   └── CookPilot_BI.pbix
│
├── bigdata/
│   ├── README.md
│   ├── cookpilot_bigdata_spark.py
│   │
│   ├── resultados/
│   │   ├── A_recorrido_mensual.csv
│   │   ├── B_actividad_mensual.csv
│   │   ├── B_usuarios_mensual.csv
│   │   ├── B_perfiles_usuarios.csv
│   │   ├── B_recurrencia_mensual.csv
│   │   ├── C_categoria_mes.csv
│   │   ├── C_categorias.csv
│   │   ├── C_mensual.csv
│   │   └── reporte_ejecucion.json
│   │
│   ├── graficos/
│   │   ├── A_recorrido_culinario.png
│   │   ├── B_comportamiento_usuarios.png
│   │   ├── B_recurrencia.png
│   │   └── C_finalizacion_por_categoria.png
│   │
│   └── experimentos/
│       └── reportes_pruebas_bigdata.zip
│
└── evidences/
    ├── bi/
    │   ├── modelo_semantico.png
    │   ├── operaciones_olap.png
    │   ├── panel_planificacion_nutricion.png
    │   ├── panel_compras_ingredientes.png
    │   └── panel_sesiones_cocina.png
    │
    └── sql/
        ├── modelo_dimensional.png
        ├── ejecucion_etl.png
        └── validacion_etl.png
```

Los dos archivos de `database/` están gestionados mediante Git LFS debido a su tamaño.

## 4. Descargar el proyecto

Se requiere acceso autorizado al repositorio privado y tener instalados Git y Git LFS.

Desde PowerShell:

```powershell
git lfs install
```

```powershell
git clone https://github.com/Paz1CP/CookPilot-BI-BigData.git
```

```powershell
cd CookPilot-BI-BigData
```

```powershell
git lfs pull
```

Después de clonar, verificar que ambos archivos de `database/` hayan sido descargados completamente y no contengan únicamente los punteros de Git LFS.

```powershell
Get-Item ".\database\CookPilot_20261009_093937.bak", ".\database\CookPilot_Schema_And_Data.sql" | Select-Object Name,Length
```

Los tamaños registrados de los archivos originales son:

- Respaldo SQL Server: **118 616 064 bytes**.
- Script SQL con esquema y datos: **369 657 142 bytes**.

## 5. Preparación de SQL Server

### 5.1. Requisitos

Instalar SQL Server 2022, preferentemente Developer para el entorno académico, y SQL Server Management Studio (SSMS).

Se necesita una instancia local accesible mediante autenticación de Windows.

El entorno original utiliza:

- Servidor SQL: `localhost`.
- Base de datos: `CookPilot`.
- Puerto TCP para Spark: `1433`.
- Autenticación: integrada de Windows.

Si SQL Server utiliza otro puerto o instancia, será necesario ajustar la conexión para Power BI y Spark. El programa ETL actual utiliza los valores de servidor y base definidos dentro de su código.

### 5.2. Restaurar la base de datos

**Método recomendado: archivo `.bak`.**

El archivo `database/CookPilot_20261009_093937.bak` contiene la base de datos preparada para el proyecto, incluyendo la estructura relacional, los registros operacionales y las tablas del almacén dimensional.

En SSMS:

1. Conectarse a SQL Server.
2. Abrir el menú contextual de **Databases**.
3. Seleccionar **Restore Database**.
4. En **Source**, seleccionar **Device** y localizar el archivo `.bak`.
5. Establecer `CookPilot` como base de destino.
6. Revisar la sección **Files** y ajustar las rutas de los archivos de datos y registro a las ubicaciones válidas del nuevo servidor.
7. Confirmar la restauración.

Si la base `CookPilot` ya existe, no sobrescribirla sin un respaldo previo y una decisión explícita.

El archivo debe encontrarse en una ruta accesible para el servicio de SQL Server. Si es necesario, copiarlo a una carpeta local que tenga los permisos correspondientes.

Puede comprobarse el respaldo mediante SQL:

```sql
RESTORE VERIFYONLY
FROM DISK = N'C:\ruta\CookPilot_20261009_093937.bak'
WITH CHECKSUM;
```

Sustituir `C:\ruta\` por la ubicación real del archivo.

Esta comprobación no reemplaza una restauración efectiva, pero permite verificar determinadas condiciones de integridad y legibilidad del respaldo.

### 5.3. Alternativa: importar el script SQL

También se incluye:

`database/CookPilot_Schema_And_Data.sql`

El archivo permite disponer de la estructura y los datos exportados en formato SQL.

Para utilizarlo, debe comprobarse primero el encabezado del script, especialmente las instrucciones de creación o selección de base de datos.

Puede ejecutarse desde SSMS o mediante `sqlcmd`, siempre que el destino sea una instancia compatible y el contexto de base de datos sea el correcto:

```powershell
sqlcmd -S localhost -E -b -i ".\database\CookPilot_Schema_And_Data.sql"
```

`-E` utiliza autenticación integrada de Windows y `-b` permite devolver un código de error cuando SQL Server informa de un fallo de ejecución.

**No es necesario aplicar el `.sql` después de restaurar el `.bak`.** Son dos alternativas para disponer de la base. Ejecutarlas ambas sobre las mismas tablas podría provocar conflictos o registros duplicados.

### 5.4. Comprobar los datos

En SSMS ejecutar:

```sql
USE CookPilot;

SELECT
    (SELECT COUNT(*) FROM dbo.fact_planning) AS planificaciones,
    (SELECT COUNT(*) FROM dbo.fact_shopping_item) AS productos_compra,
    (SELECT COUNT(*) FROM dbo.fact_cooking_session) AS sesiones_cocina,
    (SELECT COUNT(*) FROM dbo.dim_user) AS usuarios,
    (SELECT COUNT(*) FROM dbo.dim_date) AS fechas;
```

Para el conjunto evaluado:

| Entidad | Total esperado |
|---|---:|
| Planificaciones | 23 769 |
| Productos de compra | 144 829 |
| Sesiones de cocina | 13 093 |
| Usuarios sintéticos | 100 |
| Fechas | 185 |

El almacén dimensional utiliza tres tablas de hechos y nueve dimensiones dentro del esquema `dbo`.

## 6. Proceso ETL y generación de datos

El programa ETL del proyecto es:

`etl/seed_cookpilot_sql.py`

Sus funciones principales son:

- Leer información de catálogo desde una exportación SQL de PostgreSQL.
- Transformar los registros para el modelo de SQL Server.
- Generar actividad sintética para 100 usuarios mediante una semilla fija.
- Poblar las tablas operacionales y el almacén dimensional.
- Validar tablas, columnas, claves primarias, relaciones y recuentos.
- Confirmar la carga mediante una transacción, con reversión ante errores.

La simulación utiliza la semilla `20260923` y el periodo comprendido entre el 23 de marzo y el 23 de septiembre de 2026.

### 6.1. Dependencias del ETL

Se requiere Python y un controlador ODBC compatible con SQL Server. El programa prefiere Microsoft ODBC Driver 18 o 17.

Instalar `pyodbc`:

```powershell
python -m pip install pyodbc
```

El script también puede intentar instalar esta dependencia automáticamente si no la encuentra.

### 6.2. Origen requerido para reconstruir los datos

El ETL utiliza una exportación PostgreSQL original. En el entorno donde se desarrolló, buscaba preferentemente:

`backup_cookpilot_prod_2026-10-03_16-04-24.sql`

En ausencia de ese archivo, puede buscar una exportación `backup_cookpilot_dev_*.sql` en la carpeta Descargas.

**El archivo PostgreSQL original no forma parte de este repositorio.** Por tanto, el script ETL documenta el método utilizado, pero reconstruir la carga directamente desde su fuente original requiere disponer de esa exportación.

Para reproducir los análisis sin depender de dicha fuente externa, se recomienda restaurar el respaldo SQL Server incluido en `database/`.

### 6.3. Ejecutar el ETL desde su fuente original

Este procedimiento es **opcional y destructivo para los datos actuales**: el programa elimina y sustituye los registros de las tablas que administra antes de confirmar una carga válida.

Solo debe ejecutarse sobre una base preparada para reconstrucción, con respaldo previo, esquema compatible y archivo PostgreSQL de origen disponible.

Desde la raíz del repositorio:

```powershell
python ".\etl\seed_cookpilot_sql.py" --dump "C:\ruta\backup_cookpilot_prod_2026-10-03_16-04-24.sql" --confirm-reset
```

`--dump` indica la ubicación de la exportación original y `--confirm-reset` autoriza explícitamente la sustitución transaccional de los registros.

El programa verifica la estructura antes de modificar los datos y utiliza `COMMIT` o `ROLLBACK` según el resultado de las validaciones.

En el entorno original, el reporte ETL se guardaba como `cookpilot_etl_report.json` en la carpeta Descargas. No se incluye una copia de ese reporte dentro de este repositorio.

**Si se restauró el `.bak` proporcionado, no hace falta ejecutar nuevamente el ETL.** La base ya contiene los datos procesados utilizados en el proyecto.

## 7. Solución de Inteligencia de Negocios

El informe se encuentra en:

`bi/CookPilot_BI.pbix`

Para abrirlo:

1. Instalar Power BI Desktop.
2. Abrir `CookPilot_BI.pbix`.
3. Comprobar los orígenes de datos de SQL Server.
4. Si el servidor del equipo es diferente, actualizar la conexión desde **Archivo → Opciones y configuración → Configuración de origen de datos**.
5. Seleccionar las credenciales apropiadas y actualizar el modelo si es necesario.

El informe contiene tres páginas:

| Página | Contenido |
|---|---|
| Planificación y nutrición | Actividad de planificación e indicadores nutricionales |
| Compras e ingredientes | Productos de listas de compra y comportamiento de adquisición |
| Sesiones de cocina | Sesiones, duración y finalización |

También incluye indicadores para doce preguntas de negocio y permite efectuar operaciones OLAP de navegación jerárquica y filtrado.

Las capturas de los paneles, el modelo semántico y las operaciones OLAP están disponibles en `evidences/bi/`.

## 8. Solución Big Data

El programa se encuentra en:

`bigdata/cookpilot_bigdata_spark.py`

Se ejecuta mediante Apache Spark 4.2.0 y utiliza JDBC para consultar SQL Server en modo de solo lectura.

Antes de iniciar, es necesario configurar Java, Spark, el controlador JDBC Microsoft y su biblioteca de autenticación nativa.

**Las instrucciones completas de instalación, configuración, ejecución y reproducción de pruebas se encuentran en [bigdata/README.md](bigdata/README.md).**

Una vez preparado el entorno, desde la raíz del repositorio puede ejecutarse:

```powershell
$env:JAVA_TOOL_OPTIONS="-Djava.library.path=C:\spark\jdbc"; $env:Path="C:\spark\jdbc;$env:Path"; spark-submit --driver-library-path "C:\spark\jdbc" ".\bigdata\cookpilot_bigdata_spark.py"
```

La aplicación realiza los tres análisis:

- **A:** recorrido entre planificación, compras y cocina.
- **B:** actividad y recurrencia de usuarios.
- **C:** finalización y duración de sesiones.

La ejecución genera archivos CSV, gráficos PNG y reportes JSON. Las salidas obtenidas durante la evaluación están incluidas en el repositorio.

## 9. Paquete experimental y evidencias

Las evidencias técnicas pueden localizarse directamente mediante las siguientes rutas:

| Evidencia | Ubicación |
|---|---|
| Base de datos completa | `database/CookPilot_20261009_093937.bak` |
| Esquema y datos exportados | `database/CookPilot_Schema_And_Data.sql` |
| Programa ETL | `etl/seed_cookpilot_sql.py` |
| Informe Power BI | `bi/CookPilot_BI.pbix` |
| Programa Big Data | `bigdata/cookpilot_bigdata_spark.py` |
| Resultados tabulares Big Data | `bigdata/resultados/` |
| Visualizaciones Big Data | `bigdata/graficos/` |
| Reporte de ejecución Spark | `bigdata/resultados/reporte_ejecucion.json` |
| Reportes experimentales | `bigdata/experimentos/reportes_pruebas_bigdata.zip` |
| Evidencias BI | `evidences/bi/` |
| Evidencias SQL y ETL | `evidences/sql/` |

### Experimentos realizados

Se realizaron nueve pruebas de rendimiento, distribuidas entre tres configuraciones de Spark, además de una prueba de fallo controlado y una recuperación manual.

| Núcleos | Particiones | Tiempo promedio |
|---:|---:|---:|
| 1 | 1 | 30,41 s |
| 2 | 4 | 30,51 s |
| 4 | 8 | 27,70 s |

Las nueve ejecuciones comparativas fueron completadas correctamente.

La configuración de cuatro núcleos y ocho particiones obtuvo el menor tiempo promedio, aproximadamente un 8,9 % inferior al escenario de referencia. Los experimentos corresponden a una sola computadora y no demuestran escalabilidad en un clúster multinodo.

Los reportes originales se encuentran en `bigdata/experimentos/reportes_pruebas_bigdata.zip`.

## 10. . Alcance y limitaciones

La implementación corresponde a un entorno académico local con datos sintéticos.

El almacenamiento persistente se encuentra en SQL Server, el análisis multidimensional en Power BI y el procesamiento complementario en Apache Spark.

No se implementaron servicios en la nube, ingesta en tiempo real, almacenamiento distribuido, un clúster multinodo ni recuperación automática ante fallos.

Las pruebas y visualizaciones demuestran el funcionamiento de las soluciones desarrolladas, pero los resultados de actividad y comportamiento no deben interpretarse como métricas comerciales obtenidas de usuarios reales.

Los archivos de base de datos y los resultados que contienen identificadores deben revisarse antes de distribuirse fuera del entorno autorizado del proyecto.