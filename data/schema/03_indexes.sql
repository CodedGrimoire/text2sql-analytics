-- Auto-generated indexes (dynamic)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS gin_mainsheet_customerid ON mainsheet USING GIN (customerid gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_shippername ON mainsheet USING GIN (shippername gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_companyname ON mainsheet USING GIN (companyname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_contactname ON mainsheet USING GIN (contactname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_city ON mainsheet USING GIN (city gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_country ON mainsheet USING GIN (country gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_productname ON mainsheet USING GIN (productname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_suppliername ON mainsheet USING GIN (suppliername gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_categoryname ON mainsheet USING GIN (categoryname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_quantityperunit ON mainsheet USING GIN (quantityperunit gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_lastname ON mainsheet USING GIN (lastname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_firstname ON mainsheet USING GIN (firstname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_mainsheet_title ON mainsheet USING GIN (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_sheet1_beverages ON sheet1 USING GIN (beverages gin_trgm_ops);