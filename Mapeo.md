# Mapeo, lógica y decisiones — Azure Data Factory, pipeline de pedidos

El JSON en `pipeline/`, `dataflow/`, `dataset/`, `linkedService/` y `trigger/` es el comportamiento versionado. Esta nota describe el diseño del mapeo. Algunas expresiones de abajo, como el respaldo de `Qty` y el parámetro `df_fileName`, no están conectadas en `dataflow/df_orders_etl.json`.

## Resumen

Este documento describe el **mapeo de campos**, la **lógica de transformación** en ADF (Flujos de datos), las **decisiones de diseño**, los **desafíos encontrados** y cómo se resolvieron. La solución ingiere CSVs en `raw/AAAA/MM/DD/`, deriva columnas, valida, enriquece y carga filas válidas a `dbo.Orders`; las inválidas van a `rejects/` y se notifica por correo.

## Alcance y supuestos

* Formato de entrada: CSV con encabezados y columnas: ProductID, ProductDescription, Quantity, UOM\_ID, ForDate, AddressID, ContactID, Sale.
* Nombre físico del archivo: `<Cliente>_DeliveryOrder_<timestamp>.csv`.
* Timestamp en formato `yyyyMMddTHHmmssZ`.
* Diccionario adicional: CSV con `IDProduct` y `Sale Cost (USD)` (opcional).
* Las fechas del CSV (ForDate) vienen como `d/M/yyyy`.

## Datasets y orígenes

* **Pedidos (CSV)**: ADLS Gen2, contenedor `raw`. En Origen se habilita “Columna para almacenar el nombre de archivo” = `RawPath`.
* **Diccionario (CSV)**: ADLS Gen2, contenedor `dict` (o similar).
* **SQL Sink**: Azure SQL Database (tabla `dbo.Orders`).
* **ADLS rejects**: salida de rechazados en contenedor `rejects`.

## Mapeo de columnas (CSV → SQL)

| Columna CSV        | Transformación                                                                             | Destino en SQL     | Detalle                  |
| ------------------ | ------------------------------------------------------------------------------------------ | ------------------ | ------------------------ |
| ProductID          | Directa                                                                                    | ProductID          | nvarchar(50)             |
| ProductDescription | Directa                                                                                    | ProductDescription | nvarchar(200)            |
| Quantity           | `iif(isNull(byName('Quantity')), toInteger(byName('Qty')), toInteger(byName('Quantity')))` | Quantity           | int; acepta `Qty` legacy |
| UOM\_ID            | Directa                                                                                    | UOM\_ID            | nvarchar(20)             |
| ForDate            | `toDate(byName('ForDate'),'d/M/yyyy')`                                                     | ForDate            | date                     |
| AddressID          | Directa                                                                                    | AddressID          | nvarchar(50)             |
| ContactID          | Directa                                                                                    | ContactID          | nvarchar(50)             |
| Sale               | Directa                                                                                    | Sale               | decimal(18,2)            |
| (Derivada)         | `substringIndex(RawPath,'/',-1)`                                                           | FileName           | nvarchar(260)            |
| (Derivada)         | `substringIndex(FileName,'_',1)`                                                           | CustomerName       | nvarchar(100)            |
| (Derivada)         | ver “Derivaciones”                                                                         | EmailTimestamp     | datetime2(0)             |

La expresión versionada de `Quantity` en `dataflow/df_orders_etl.json` lee solo `Quantity`: `iif(isNull(Quantity) || trim(Quantity)=='', toInteger(null()), toInteger(Quantity))`. `scripts/validate_adf.py` trata una columna `Quantity` ausente como nula. `Qty` permanece en la fila y no se copia a `Quantity`. El origen del diccionario tiene `ignoreNoFilesFound: false`, así que la falta de `dict/Dict_USD.csv` detiene el flujo. Ese archivo no está en el repositorio.

### Derivaciones (expresiones de Flujo de datos)

* FileName

  ```
  substringIndex(RawPath,'/',-1)
  ```
* CustomerName

  ```
  substringIndex(FileName,'_',1)
  ```
* EmailTimestamp

  ```
  toTimestamp(
    replace(split(replace(FileName,'.csv',''),'_')[2],'-',':'),
    'yyyyMMdd\'T\'HHmmss\'Z\''
  )
  ```

## Enriquecimiento (opcional)

* Lookup con **Diccionario** por clave `ProductID == IDProduct` para traer `Sale Cost (USD)` u otros atributos.

## Validación y partición de filas

* **good\_rows**:

  ```
  not(isNull(Quantity))
  ```
* **bad\_rows**:

  ```
  isNull(Quantity)
  ```
* Antes del sink de rechazados se derivan:

  * RejectReason = `'Quantity is NULL'`
  * OutFileName = `CustomerName + '_rejects_' + toString(currentUTC(),'yyyyMMddHHmmss') + '_' + FileName'`

## Filtro por archivo (con parámetro)

Para procesar solo el blob que disparó el evento:

* Parámetro del flujo: `df_fileName` (Cadena).
* En canalización: variable `v_fileName` ← `p_fileName` (del trigger).
* Asignación en actividad de flujo: `df_fileName = @variables('v_fileName')`.
* Filtro en el flujo:

  ```
  upper(trim(FileName)) == upper(trim($df_fileName))
  ```

> Alternativa sin parámetro (por patrón de nombre):
>
> ```
> startsWith(
>   upper(FileName),
>   upper(substringIndex(FileName, '_', 1) + '_DeliveryOrder_')
> )
> ```

## Sinks

**SQL (dbo.Orders)**: mapear columnas de negocio + derivadas (`CustomerName`, `EmailTimestamp`, `FileName`).
**ADLS (rejects/)**: nombre de archivo desde columna `OutFileName`.

## Notificación (Logic App de alerta)

* Condición en la canalización:

  ```
  @greater(int(activity('act_df_orders_etl').output.runStatus.metrics.sinkRejects.rowsWritten), 0)
  ```
* Web (POST) → Logic App (HTTP trigger). Cuerpo:

  ```
  {
    "fileName": "@{pipeline().parameters.p_fileName}",
    "customerName": "@{variables('v_customerName')}",
    "rejects": @{activity('act_df_orders_etl').output.runStatus.metrics.sinkRejects.rowsWritten},
    "blobUrl": "@{concat('https://<storage>.blob.core.windows.net/raw/', replace(pipeline().parameters.p_folderPath,'raw/',''), '/', pipeline().parameters.p_fileName)}"
  }
  ```

## Estructura en ADLS

* **Entradas**: `raw/AAAA/MM/DD/<Cliente>_DeliveryOrder_<timestamp>.csv`
* **Rechazos**: `rejects/<particiones opcionales>/part-*.csv` con `OutFileName` y `_SUCCESS`.

## Seguridad y acceso

* Identidad administrada de ADF con rol de **Colaborador de datos del Blob de Storage** sobre el contenedor.
* Azure SQL: autenticación (MI o cadena de conexión) con permisos para `INSERT` en `dbo.Orders`.

## Observabilidad

* ADF → Supervisar: revisar `runStatus.metrics` de la actividad del flujo.
* Logic App (alerta): historial de ejecuciones (200/400, payload recibido).

## Pruebas

1. **CSV “bueno”**: filas en `dbo.Orders`, sin rechazados, sin correo.
2. **CSV con nulos en Quantity**: archivo en `rejects/` y correo de alerta.
3. **Multiples clientes** el mismo día: cada evento procesa su archivo (filtro por `df_fileName`).

## Desafíos y soluciones

* **Path no resuelve**: usar ruta completa o comodines correctos; evitar nombres con caracteres especiales.
* **Funciones de cadena**: `indexOf` no disponible; usar `substringIndex`, `split`, `upper`, `startsWith`.
* **Tipo de expresión no evaluable**: pasar `p_fileName` → variable `v_fileName` → `df_fileName` (evita problemas de tipado).
* **Encabezado como fila**: activar “Primera fila tiene encabezados” o “Omitir recuento de líneas = 1”.
* **Cuerpo de Web no válido**: enviar JSON (no URL plana); añadir `Content-Type: application/json`.
* **Links con `raw/raw`**: normalizar con `replace(p_folderPath,'raw/','')`.
* **Cold start del cluster**: primer debug/job tarda más; luego mejora.

## Decisiones de diseño

* **Filtro por archivo** para idempotencia y trazabilidad.
* **Rejects particionados** y con `_SUCCESS` para auditoría.
* **EmailTimestamp** desde el nombre físico para consistencia temporal.
* **Diccionario externo** desacoplado para costos/atributos.

## Mejoras futuras

* Deduplicación por `FileName` en SQL.
* Carga incremental por partición.
* Métricas a Log Analytics / Application Insights.
* Reglas de validación adicionales (rango de fechas, catálogo de productos, etc.).

## Consultas útiles (SQL)

```
SELECT COUNT(*) Total, MIN(EmailTimestamp) MinTs, MAX(EmailTimestamp) MaxTs
FROM dbo.Orders;

SELECT TOP 50 *
FROM dbo.Orders
ORDER BY EmailTimestamp DESC;

SELECT TOP 50 *
FROM dbo.OrdersRejects
ORDER BY LoadTime DESC;
```
