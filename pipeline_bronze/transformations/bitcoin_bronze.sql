CREATE OR REFRESH STREAMING TABLE bitcoin
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT * FROM cloud_files(
  '/Volumes/lakehouse/raw_public/coinbase/coinbase/bitcoin_spot/', 'json',
  map(
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.includeExistingFiles', 'false',
    'cloudFiles.schemaEvolutionMode', 'addNewColumns'
  )
);