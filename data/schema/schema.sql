CREATE TABLE Customers (
            CustomerID VARCHAR PRIMARY KEY,
            CompanyName VARCHAR NOT NULL,
            ContactName VARCHAR,
            Country VARCHAR
        );

        CREATE TABLE Employees (
            EmployeeID SERIAL PRIMARY KEY,
            LastName VARCHAR,
            FirstName VARCHAR,
            Title VARCHAR
        );

        CREATE TABLE Orders (
            OrderID SERIAL PRIMARY KEY,
            CustomerID VARCHAR REFERENCES Customers(CustomerID),
            EmployeeID INT REFERENCES Employees(EmployeeID),
            OrderDate DATE,
            ShippedDate DATE,
            ShipVia INT,
            Freight NUMERIC
        );

        CREATE TABLE Products (
            ProductID SERIAL PRIMARY KEY,
            ProductName VARCHAR NOT NULL,
            SupplierID INT,
            CategoryID INT,
            QuantityPerUnit VARCHAR,
            UnitPrice NUMERIC,
            UnitsInStock INT,
            Discontinued BOOLEAN
        );

        CREATE TABLE OrderDetails (
            OrderID INT REFERENCES Orders(OrderID),
            ProductID INT REFERENCES Products(ProductID),
            UnitPrice NUMERIC,
            Quantity INT,
            Discount NUMERIC,
            PRIMARY KEY (OrderID, ProductID)
        );

        CREATE INDEX idx_orders_customer ON Orders(CustomerID);
        CREATE INDEX idx_orders_employee ON Orders(EmployeeID);
        CREATE INDEX idx_orderdetails_product ON OrderDetails(ProductID);