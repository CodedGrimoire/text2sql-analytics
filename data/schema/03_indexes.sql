-- B-tree on common joins/filters
CREATE INDEX IF NOT EXISTS idx_orders_customerid       ON orders(customerid);
CREATE INDEX IF NOT EXISTS idx_orders_employeeid       ON orders(employeeid);
CREATE INDEX IF NOT EXISTS idx_orders_orderdate        ON orders(orderdate);
CREATE INDEX IF NOT EXISTS idx_orderdetails_productid  ON orderdetails(productid);
CREATE INDEX IF NOT EXISTS idx_products_categoryid     ON products(categoryid);
CREATE INDEX IF NOT EXISTS idx_products_supplierid     ON products(supplierid);

-- Optional text search helpers (requires pg_trgm)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS gin_customers_companyname ON customers USING GIN (companyname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS gin_products_productname  ON products  USING GIN (productname  gin_trgm_ops);
