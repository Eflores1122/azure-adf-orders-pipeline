# Mapping, logic and design — Azure Data Factory orders pipeline

The JSON under `pipeline/`, `dataflow/`, `dataset/`, `linkedService/`, and `trigger/` is the behavior that is checked in. This note is the field-mapping design. A few expressions below, including a `Qty` fallback and a `df_fileName` parameter, are design notes and are not wired in `dataflow/df_orders_etl.json`.

## Summary

This document details the **field mapping**, **data flow logic**, **design decisions**, **challenges**, and resolutions. CSV files land under `raw/YYYY/MM/DD/`, derived columns are computed, rows are validated and enriched, valid rows go to `dbo.Orders`, rejects to `rejects/`, and an email alert is sent when rejects exist.

## Scope & assumptions

* Input CSV with headers: ProductID, ProductDescription, Quantity, UOM\_ID, ForDate, AddressID, ContactID, Sale.
* Physical file name: `<Customer>_DeliveryOrder_<timestamp>.csv`.
* Timestamp format: `yyyyMMddTHHmmssZ`.
* Optional dictionary CSV with `IDProduct` and `Sale Cost (USD)`.
* CSV date format: `d/M/yyyy`.

## Datasets & sources

* **Orders (CSV)**: ADLS Gen2 `raw`, capture file name in `RawPath`.
* **Dictionary (CSV)**: ADLS `dict` (or similar).
* **SQL Sink**: Azure SQL (`dbo.Orders`).
* **ADLS rejects**: output for invalid rows.

## Column mapping (CSV → SQL)

| CSV column         | Transformation                                                                             | SQL target         | Notes         |
| ------------------ | ------------------------------------------------------------------------------------------ | ------------------ | ------------- |
| ProductID          | Direct                                                                                     | ProductID          | nvarchar(50)  |
| ProductDescription | Direct                                                                                     | ProductDescription | nvarchar(200) |
| Quantity           | `iif(isNull(byName('Quantity')), toInteger(byName('Qty')), toInteger(byName('Quantity')))` | Quantity           | int           |
| UOM\_ID            | Direct                                                                                     | UOM\_ID            | nvarchar(20)  |
| ForDate            | `toDate(byName('ForDate'),'d/M/yyyy')`                                                     | ForDate            | date          |
| AddressID          | Direct                                                                                     | AddressID          | nvarchar(50)  |
| ContactID          | Direct                                                                                     | ContactID          | nvarchar(50)  |
| Sale               | Direct                                                                                     | Sale               | decimal(18,2) |
| (Derived)          | `substringIndex(RawPath,'/',-1)`                                                           | FileName           | nvarchar(260) |
| (Derived)          | `substringIndex(FileName,'_',1)`                                                           | CustomerName       | nvarchar(100) |
| (Derived)          | see “Derivations”                                                                          | EmailTimestamp     | datetime2(0)  |

### Derivations (Data Flow expressions)

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

## Enrichment (optional)

* Lookup `ProductID == IDProduct` to bring `Sale Cost (USD)` or other attributes.

## Validation & row split

* **good\_rows**:

  ```
  not(isNull(Quantity))
  ```
* **bad\_rows**:

  ```
  isNull(Quantity)
  ```
* Before reject sink:

  * RejectReason = `'Quantity is NULL'`
  * OutFileName = `CustomerName + '_rejects_' + toString(currentUTC(),'yyyyMMddHHmmss') + '_' + FileName'`

## File-level filtering (parameterized)

* Flow parameter: `df_fileName` (String).
* Pipeline variable `v_fileName` set from `p_fileName`.
* Assign `df_fileName = @variables('v_fileName')` in the Data Flow activity.
* Filter expression:

  ```
  upper(trim(FileName)) == upper(trim($df_fileName))
  ```

> Pattern-only alternative:
>
> ```
> startsWith(
>   upper(FileName),
>   upper(substringIndex(FileName, '_', 1) + '_DeliveryOrder_')
> )
> ```

## Sinks

**SQL (dbo.Orders)**: map business columns plus `CustomerName`, `EmailTimestamp`, `FileName`.
**ADLS (rejects/)**: use `OutFileName`.

## Notification (Logic App)

* If:

  ```
  @greater(int(activity('act_df_orders_etl').output.runStatus.metrics.sinkRejects.rowsWritten), 0)
  ```
* Web POST body:

  ```
  {
    "fileName": "@{pipeline().parameters.p_fileName}",
    "customerName": "@{variables('v_customerName')}",
    "rejects": @{activity('act_df_orders_etl').output.runStatus.metrics.sinkRejects.rowsWritten},
    "blobUrl": "@{concat('https://<storage>.blob.core.windows.net/raw/', replace(pipeline().parameters.p_folderPath,'raw/',''), '/', pipeline().parameters.p_fileName)}"
  }
  ```

## ADLS layout

* Inputs: `raw/YYYY/MM/DD/<Customer>_DeliveryOrder_<timestamp>.csv`
* Rejects: `rejects/.../part-*.csv` plus `_SUCCESS`

## Security & access

* ADF Managed Identity with **Storage Blob Data Contributor** on the container.
* Azure SQL auth (MI or connection string) with INSERT into `dbo.Orders`.

## Observability

* ADF Monitor → check `runStatus.metrics`.
* Logic App (alert) run history and payloads.

## Testing

1. **Good CSV**: rows in `dbo.Orders`, no rejects, no email.
2. **CSV with nulls**: file written in `rejects/`, alert email sent.
3. Multiple customers same day: each event processes its own file (`df_fileName` filter).

## Challenges & resolutions

* Path resolution, function availability (`substringIndex`, `upper`, `startsWith`), header row handling, pipeline vs data flow expression contexts, JSON body for Web activity, normalizing `blobUrl`, and cluster cold start.

## Design decisions

* File-level filtering for idempotency, rejects with auditability, `EmailTimestamp` derived from file name, optional external dictionary.

## Future improvements

Deduplication, partition-aware loads, observability to Log Analytics, stronger validation rules.

## Handy SQL

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
