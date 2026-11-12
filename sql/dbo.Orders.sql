-- Columns written by the Orders sink in dataflow/df_orders_etl.json.
-- Synthetic portfolio DDL. Apply it on Azure SQL before publishing the factory.
-- CI reads this file as text and does not open a database connection.

CREATE TABLE dbo.Orders (
  ProductID           nvarchar(50)   NOT NULL,
  ProductDescription  nvarchar(200)  NOT NULL,
  Quantity            int            NULL,
  UOM_ID              nvarchar(20)   NULL,
  ForDate             date           NULL,
  AddressID           nvarchar(50)   NULL,
  ContactID           nvarchar(50)   NULL,
  Sale                decimal(18,2)  NULL,
  RawPath             nvarchar(1000) NULL,
  FileName            nvarchar(260)  NULL,
  CustomerName        nvarchar(100)  NULL,
  EmailTimestamp      datetime2(0)   NULL,
  IDProduct           nvarchar(50)   NULL,
  [Sale Cost (USD)]   decimal(18,4)  NULL
);
