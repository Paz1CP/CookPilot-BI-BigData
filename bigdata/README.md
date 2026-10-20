# CookPilot — Procesamiento Big Data con Apache Spark

## 1. Descripción

Esta implementación utiliza **Apache Spark 4.2.0 y PySpark** para analizar los datos de CookPilot almacenados en **SQL Server 2022**.

El procesamiento se realiza mediante JDBC, en modo local y por lotes. El programa consulta las tablas existentes sin modificar sus registros, ejecuta tres análisis y genera resultados CSV, visualizaciones PNG y un reporte JSON por ejecución.

El archivo principal es:

`bigdata/cookpilot_bigdata_spark.py`

**Análisis implementados:**

- **A — Recorrido culinario:** relaciones entre planificación, productos de compra y sesiones de cocina.
- **B — Comportamiento de usuarios:** actividad por mes, participación en procesos y recurrencia.
- **C — Sesiones de cocina:** finalización, duración y diferencias entre tiempos registrados y estimados.

La solución fue comprobada con datos sintéticos del periodo comprendido entre el 23 de marzo y el 23 de septiembre de 2026. No utiliza AWS S3, Hadoop HDFS, Kafka ni un clúster multinodo.

## 2. Requisitos

El entorno original de ejecución utilizó:

| Componente | Versión o requisito |
|---|---|
| Sistema operativo | Windows 11 |
| SQL Server | 2022 |
| Apache Spark | 4.2.0 |
| Python | 3.14.2 |
| Java JDK | 24.0.2 |
| Microsoft JDBC Driver | 13.6.0 |
| Controlador nativo JDBC | `mssql-jdbc_auth-13.6.0.x64.dll` |
| Matplotlib | Necesario para generar gráficos PNG |
| Git LFS | Necesario para descargar la base de datos |

Las versiones indicadas corresponden al entorno comprobado. No se ha validado esta implementación con todas las posibles combinaciones de versiones.

Antes de comenzar, debe existir una instancia de SQL Server con la base `CookPilot`, incluyendo sus tablas dimensionales y operacionales. Las instrucciones para restaurarla se encuentran en el README principal del repositorio.

## 3. Instalación de Apache Spark

### 3.1. Preparar Java y Python

Instalar Java JDK y Python, comprobando que sean accesibles desde PowerShell:

```powershell
java -version
```

```powershell
python --version
```

Configurar `JAVA_HOME` apuntando al directorio donde esté instalado el JDK y asegurarse de que Python y Java estén disponibles en `PATH`.

### 3.2. Preparar Spark 4.2.0

Descargar y extraer la distribución de Apache Spark 4.2.0.

El entorno original utilizó esta ubicación:

```text
C:\spark\
├── bin\
├── conf\
├── jars\
└── ...
```

Configurar Spark para la sesión de PowerShell:

```powershell
$env:SPARK_HOME="C:\spark"; $env:Path="C:\spark\bin;$env:Path"
```

Comprobar su disponibilidad:

```powershell
spark-submit --version
```

El comando debe mostrar la versión de Apache Spark instalada.

### 3.3. Instalar Matplotlib

Para generar las cuatro visualizaciones se requiere Matplotlib:

```powershell
python -m pip install matplotlib
```

Si Matplotlib no está disponible, el programa puede producir los CSV y el reporte JSON, pero no generará los PNG.

## 4. Configuración del controlador JDBC

Spark se conecta a SQL Server utilizando el controlador JDBC de Microsoft y autenticación integrada de Windows.

Es necesario descargar el Microsoft JDBC Driver 13.6.0, colocar su archivo JAR compatible con Java en la carpeta de bibliotecas de Spark y copiar la DLL de autenticación nativa correspondiente a Windows x64.

La instalación utilizada en las pruebas tuvo esta estructura:

```text
C:\spark\
├── jars\
│   └── sqlserver-jdbc.jar
│
└── jdbc\
    └── mssql-jdbc_auth-13.6.0.x64.dll
```

`sqlserver-jdbc.jar` es el nombre utilizado localmente para el archivo JAR del controlador Microsoft JDBC.

El directorio `C:\spark\jdbc` debe contener la DLL para que Java pueda localizarla durante la autenticación integrada.

No es necesario almacenar nombres de usuario ni contraseñas en el programa. La conexión utiliza la identidad de Windows que ejecuta el proceso.

## 5. Preparación de SQL Server

La conexión predeterminada del programa es:

| Parámetro | Valor |
|---|---|
| Servidor | `127.0.0.1` |
| Puerto | `1433` |
| Base de datos | `CookPilot` |
| Autenticación | Integrada de Windows |
| Tipo de conexión | JDBC |
| Operaciones sobre SQL Server | Solo lectura |

Para comprobar que el puerto responde:

```powershell
Test-NetConnection 127.0.0.1 -Port 1433
```

En SQL Server Management Studio, comprobar que la base contiene registros:

```sql
USE CookPilot;

SELECT
    (SELECT COUNT(*) FROM dbo.fact_planning) AS planificaciones,
    (SELECT COUNT(*) FROM dbo.fact_shopping_item) AS productos_compra,
    (SELECT COUNT(*) FROM dbo.fact_cooking_session) AS sesiones_cocina,
    (SELECT COUNT(*) FROM dbo.dim_date) AS fechas,
    (SELECT COUNT(*) FROM dbo.dim_user) AS usuarios;
```

Para el conjunto utilizado en el proyecto, los resultados esperados son:

| Campo | Registros |
|---|---:|
| Planificaciones | 23 769 |
| Productos de compra | 144 829 |
| Sesiones de cocina | 13 093 |
| Fechas | 185 |
| Usuarios | 100 |

Si el servidor utiliza otro nombre, puerto o base de datos, estos valores se pueden modificar mediante los parámetros del programa.

## 6. Ejecución del programa

Abrir PowerShell en la raíz del repositorio `CookPilot-BI-BigData`.

Comprobar que Spark esté disponible. Si no se encuentra en `PATH`, ejecutar:

```powershell
$env:SPARK_HOME="C:\spark"; $env:Path="C:\spark\bin;$env:Path"
```

Ejecutar el programa:

```powershell
$env:JAVA_TOOL_OPTIONS="-Djava.library.path=C:\spark\jdbc"; $env:Path="C:\spark\jdbc;$env:Path"; spark-submit --driver-library-path "C:\spark\jdbc" ".\bigdata\cookpilot_bigdata_spark.py"
```

**Explicación del comando:**

- `JAVA_TOOL_OPTIONS` establece la ubicación de la biblioteca nativa de autenticación.
- `Path` incorpora el directorio que contiene la DLL.
- `spark-submit` inicia la aplicación PySpark.
- `--driver-library-path` permite localizar la biblioteca nativa desde el proceso controlador de Spark.
- `bigdata\cookpilot_bigdata_spark.py` es el programa de análisis.

La configuración predeterminada ejecuta Spark con **dos núcleos locales y cuatro particiones de transformación**. La lectura JDBC utiliza una partición por tabla.

La conexión incluye `encrypt=true` y `trustServerCertificate=true`. El tráfico se cifra, pero la validación de la cadena de confianza del certificado del servidor no es completa; esta configuración corresponde al entorno académico local.

## 7. Resultados generados

Cada ejecución crea una carpeta identificada por su fecha y hora:

```text
bigdata/
├── cookpilot_bigdata_spark.py
├── resultados_bigdata/
│   └── ejecucion_YYYYMMDD_HHMMSS/
│       ├── A_recorrido_mensual.csv
│       ├── B_actividad_mensual.csv
│       ├── B_usuarios_mensual.csv
│       ├── B_perfiles_usuarios.csv
│       ├── B_recurrencia_mensual.csv
│       ├── C_categoria_mes.csv
│       ├── C_categorias.csv
│       ├── C_mensual.csv
│       ├── A_recorrido_culinario.png
│       ├── B_comportamiento_usuarios.png
│       ├── B_recurrencia.png
│       ├── C_finalizacion_por_categoria.png
│       └── reporte_ejecucion.json
```

La ubicación anterior corresponde a las salidas de una **nueva ejecución**, con los parámetros predeterminados. Los resultados históricos seleccionados para la entrega se encuentran en las carpetas `bigdata/resultados/` y `bigdata/graficos/` del repositorio.

El archivo `reporte_ejecucion.json` registra las configuraciones, las fuentes, los controles de calidad, los resultados, el consumo puntual de memoria JVM, los tiempos y el estado final.

Una ejecución correcta termina con:

```text
ANÁLISIS A, B Y C COMPLETADOS
```

Además, el reporte JSON debe contener:

```json
{
  "estado": "completado"
}
```

## 8. Configuración de las ejecuciones

El programa admite los siguientes parámetros:

| Parámetro | Valor predeterminado | Función |
|---|---|---|
| `--host` | `127.0.0.1` | Dirección de SQL Server |
| `--port` | `1433` | Puerto de conexión |
| `--database` | `CookPilot` | Base de datos |
| `--cores` | `2` | Núcleos de procesamiento local |
| `--partitions` | `4` | Particiones de transformación |
| `--window-days` | `7` | Ventana temporal del análisis A |
| `--output` | `bigdata/resultados_bigdata` | Directorio base de salidas |
| `--sin-graficos` | Desactivado | Omite la generación de PNG |

Por ejemplo, para utilizar cuatro núcleos y ocho particiones:

```powershell
$env:JAVA_TOOL_OPTIONS="-Djava.library.path=C:\spark\jdbc"; $env:Path="C:\spark\jdbc;$env:Path"; spark-submit --driver-library-path "C:\spark\jdbc" ".\bigdata\cookpilot_bigdata_spark.py" --cores 4 --partitions 8
```

El programa realiza las mismas operaciones analíticas, modificando la configuración de procesamiento local.

## 9. Reproducción experimental

Se evaluaron tres escenarios:

| Escenario | Núcleos | Particiones | Repeticiones | Promedio registrado |
|---|---:|---:|---:|---:|
| EXP-BD-01 | 1 | 1 | 3 | 30,41 s |
| EXP-BD-02 | 2 | 4 | 3 | 30,51 s |
| EXP-BD-03 | 4 | 8 | 3 | 27,70 s |

Los once reportes JSON originales, correspondientes a nueve ejecuciones comparativas, un fallo controlado y una recuperación manual, se encuentran en:

`bigdata/experimentos/reportes_pruebas_bigdata.zip`

Para repetir las nueve pruebas desde la raíz del repositorio:

```powershell
$env:JAVA_TOOL_OPTIONS="-Djava.library.path=C:\spark\jdbc"; $env:Path="C:\spark\jdbc;$env:Path"; foreach ($cfg in @(@(1,1),@(2,4),@(4,8))) { foreach ($rep in 1..3) { Write-Host "PRUEBA: cores=$($cfg[0]) particiones=$($cfg[1]) repeticion=$rep"; spark-submit --driver-library-path "C:\spark\jdbc" ".\bigdata\cookpilot_bigdata_spark.py" --cores $cfg[0] --partitions $cfg[1] --sin-graficos --output ".\pruebas_bigdata_locales"; if ($LASTEXITCODE -ne 0) { throw "Fallo la prueba de rendimiento" } } }
```

La opción `--sin-graficos` evita que la generación de PNG altere la comparación de tiempos de procesamiento.

Las duraciones pueden variar entre equipos, incluso con la misma configuración.

## 10. Prueba de fallo y recuperación

La prueba de resiliencia utilizó intencionalmente un puerto incorrecto.

**Fallo controlado:**

```powershell
$env:JAVA_TOOL_OPTIONS="-Djava.library.path=C:\spark\jdbc"; $env:Path="C:\spark\jdbc;$env:Path"; spark-submit --driver-library-path "C:\spark\jdbc" ".\bigdata\cookpilot_bigdata_spark.py" --port 1 --sin-graficos --output ".\pruebas_bigdata_locales"; Write-Host "Codigo de salida: $LASTEXITCODE"
```

Se espera un código de salida distinto de cero, acompañado de un reporte con estado `fallido`.

**Recuperación manual:**

```powershell
$env:JAVA_TOOL_OPTIONS="-Djava.library.path=C:\spark\jdbc"; $env:Path="C:\spark\jdbc;$env:Path"; spark-submit --driver-library-path "C:\spark\jdbc" ".\bigdata\cookpilot_bigdata_spark.py" --port 1433 --sin-graficos --output ".\pruebas_bigdata_locales"; Write-Host "Codigo de salida: $LASTEXITCODE"
```

El resultado esperado es un código 0 y la finalización de los análisis.

Esta prueba demuestra detección de errores y recuperación mediante una nueva ejecución; no constituye una implementación de recuperación automática ni alta disponibilidad.

## 11. Limitaciones

Esta implementación corresponde a una demostración académica con datos sintéticos.

- Spark se ejecuta sobre una computadora, no en un clúster multinodo.
- La ingesta es por lotes, no en tiempo real.
- El programa no modifica ni vuelve a guardar los datos en SQL Server.
- La relación entre planificación y compras es verificable mediante los registros de origen.
- La relación entre planificación y sesiones de cocina es una asociación temporal estimada, no una conversión causal demostrada.
- Los resultados no permiten inferir comportamientos comerciales de usuarios reales.
- No se implementaron almacenamiento distribuido, orquestación automática ni servicios en la nube.

La ejecución histórica comprobada el 9 de octubre de 2026 terminó correctamente en **36,75 segundos**, procesando los tres análisis y generando ocho CSV, cuatro gráficos y su reporte JSON.

**Versión del programa: 1.0.0.**