-- Normalized Northwind-ish schema (3NF), idempotent
CREATE TABLE IF NOT EXISTS customers (
  customerid        VARCHAR(10) PRIMARY KEY,
  companyname       VARCHAR(100) NOT NULL,
  contactname       VARCHAR(100),
  contacttitle      VARCHAR(50),
  address           VARCHAR(120),
  city              VARCHAR(50),
  region            VARCHAR(50),
  postalcode        VARCHAR(20),
  country           VARCHAR(50),
  phone             VARCHAR(30),
  fax               VARCHAR(30),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS suppliers (
  supplierid        SERIAL PRIMARY KEY,
  companyname       VARCHAR(100) NOT NULL,
  contactname       VARCHAR(100),
  contacttitle      VARCHAR(50),
  address           VARCHAR(120),
  city              VARCHAR(50),
  region            VARCHAR(50),
  postalcode        VARCHAR(20),
  country           VARCHAR(50),
  phone             VARCHAR(30),
  fax               VARCHAR(30),
  homepage          TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS categories (
  categoryid        SERIAL PRIMARY KEY,
  categoryname      VARCHAR(50)  NOT NULL UNIQUE,
  description       TEXT,
  picture           BYTEA,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shippers (
  shipperid         SERIAL PRIMARY KEY,
  companyname       VARCHAR(100) NOT NULL,
  phone             VARCHAR(30),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS employees (
  employeeid        SERIAL PRIMARY KEY,
  lastname          VARCHAR(20)  NOT NULL,
  firstname         VARCHAR(10)  NOT NULL,
  title             VARCHAR(30),
  titleofcourtesy   VARCHAR(25),
  birthdate         DATE,
  hiredate          DATE,
  address           VARCHAR(120),
  city              VARCHAR(50),
  region            VARCHAR(50),
  postalcode        VARCHAR(20),
  country           VARCHAR(50),
  homephone         VARCHAR(30),
  extension         VARCHAR(10),
  notes             TEXT,
  reportsto         INT,
  photopath         VARCHAR(255),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fk_employees_reportsto
    FOREIGN KEY (reportsto) REFERENCES employees(employeeid)
    ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS products (
  productid         SERIAL PRIMARY KEY,
  productname       VARCHAR(100) NOT NULL,
  supplierid        INT REFERENCES suppliers(supplierid) ON UPDATE CASCADE ON DELETE RESTRICT,
  categoryid        INT REFERENCES categories(categoryid) ON UPDATE CASCADE ON DELETE SET NULL,
  quantityperunit   VARCHAR(50),
  unitprice         NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (unitprice >= 0),
  unitsinstock      INT NOT NULL DEFAULT 0 CHECK (unitsinstock >= 0),
  unitsonorder      INT NOT NULL DEFAULT 0 CHECK (unitsonorder >= 0),
  reorderlevel      INT NOT NULL DEFAULT 0 CHECK (reorderlevel >= 0),
  discontinued      BOOLEAN NOT NULL DEFAULT FALSE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
  orderid           SERIAL PRIMARY KEY,
  customerid        VARCHAR(10) REFERENCES customers(customerid) ON UPDATE CASCADE ON DELETE RESTRICT,
  employeeid        INT REFERENCES employees(employeeid)       ON UPDATE CASCADE ON DELETE SET NULL,
  orderdate         DATE,
  requireddate      DATE,
  shippeddate       DATE,
  shipperid         INT REFERENCES shippers(shipperid)         ON UPDATE CASCADE ON DELETE SET NULL,
  freight           NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (freight >= 0),
  shipname          VARCHAR(100),
  shipaddress       VARCHAR(120),
  shipcity          VARCHAR(50),
  shipregion        VARCHAR(50),
  shippostalcode    VARCHAR(20),
  shipcountry       VARCHAR(50),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orderdetails (
  orderid           INT NOT NULL,
  productid         INT NOT NULL,
  unitprice         NUMERIC(12,2) NOT NULL CHECK (unitprice >= 0),
  quantity          INT NOT NULL CHECK (quantity > 0),
  discount          NUMERIC(4,3) NOT NULL DEFAULT 0 CHECK (discount >= 0 AND discount <= 1),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (orderid, productid),
  CONSTRAINT fk_orderdetails_order
    FOREIGN KEY (orderid)  REFERENCES orders(orderid)   ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_orderdetails_product
    FOREIGN KEY (productid) REFERENCES products(productid) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS region (
  regionid          INT PRIMARY KEY,
  regiondescription VARCHAR(50) NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS territories (
  territoryid       VARCHAR(20) PRIMARY KEY,
  territorydescription VARCHAR(50) NOT NULL,
  regionid          INT NOT NULL REFERENCES region(regionid) ON UPDATE CASCADE ON DELETE RESTRICT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS employeeterritories (
  employeeid        INT NOT NULL REFERENCES employees(employeeid) ON UPDATE CASCADE ON DELETE CASCADE,
  territoryid       VARCHAR(20) NOT NULL REFERENCES territories(territoryid) ON UPDATE CASCADE ON DELETE CASCADE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (employeeid, territoryid)
);

CREATE TABLE IF NOT EXISTS customerdemographics (
  customertypeid    VARCHAR(10) PRIMARY KEY,
  customerdesc      TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS customercustomerdemo (
  customerid        VARCHAR(10) NOT NULL REFERENCES customers(customerid) ON DELETE CASCADE,
  customertypeid    VARCHAR(10) NOT NULL REFERENCES customerdemographics(customertypeid) ON DELETE CASCADE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (customerid, customertypeid)
);
