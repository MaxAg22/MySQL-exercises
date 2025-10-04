-- 1. Listar los 5 clientes que más ingresos han generado a lo largo del tiempo.

SELECT  c.CompanyName AS Customer,
        SUM((oD.UnitPrice * oD.Quantity) * (1 - oD.Discount)) AS Amount
FROM Customers AS c
INNER JOIN Orders AS o ON o.CustomerID = c.CustomerID
INNER JOIN `order details` AS oD ON o.OrderID = oD.OrderID
GROUP BY Customer
ORDER BY Amount DESC
LIMIT 5;

-- 2. Listar cada producto con sus ventas totales, agrupados por categoría.

-- Como no hay ningun 'estado' en las ordenes, asumo que si tengo una entrada
-- en la tabla order details, es porque se vendio

SELECT  c.CategoryName AS category, 
        p.ProductName AS productName, 
        SUM((Od.UnitPrice * Od.Quantity) * (1 - Od.Discount)) AS Amount
FROM Products AS p
INNER JOIN Categories AS c ON c.CategoryID = p.CategoryID
INNER JOIN `order details` AS Od ON Od.ProductID = p.ProductID
GROUP BY productName, category; 

-- 3. Calcular el total de ventas para cada categoría.

SELECT  c.CategoryName AS category, 
        SUM((Od.UnitPrice * Od.Quantity) * (1 - Od.Discount)) AS Amount
FROM Categories AS c
INNER JOIN Products AS p ON c.CategoryID = p.CategoryID
INNER JOIN `order details` AS Od ON Od.ProductID = p.ProductID
GROUP BY category
ORDER BY Amount DESC;

-- 4. Crear una vista que liste los empleados con más ventas por cada año, mostrando
-- empleado, año y total de ventas. Ordenar el resultado por año ascendente.

CREATE VIEW employee_list AS
WITH 
sells_per_employee AS ( 
    SELECT  e.FirstName AS name, 
        YEAR(o.OrderDate) AS year, 
        SUM((Od.UnitPrice * Od.Quantity) * (1 - Od.Discount)) AS totalSell
        FROM Employees AS e
        INNER JOIN Orders AS o ON o.EmployeeID = e.EmployeeID
        INNER JOIN `order details` AS oD ON oD.OrderID = o.OrderID
        GROUP BY year, name
    ),
max_sells_per_year AS (
    SELECT spe.year AS year, MAX(spe.totalSell) AS maxx
    FROM sells_per_employee AS spe
    GROUP BY spe.year
    )
SELECT spe.name, spe.year, spe.totalSell
FROM sells_per_employee AS spe
INNER JOIN max_sells_per_year AS mspe
ON mspe.year = spe.year AND mspe.maxx = spe.totalSell
ORDER BY spe.year ASC;

-- que manera de sufrir

-- 5. Crear un trigger que se ejecute después de insertar un nuevo registro en la tabla
-- Order Details. Este trigger debe actualizar la tabla Products para disminuir la
-- cantidad en stock (UnitsInStock) del producto correspondiente, restando la
-- cantidad (Quantity) que se acaba de insertar en el detalle del pedido.

DELIMITER $$

CREATE TRIGGER update_products after INSERT
ON `order details` FOR EACH ROW
BEGIN 

    UPDATE Products
    SET UnitsInStock = UnitsInStock - NEW.Quantity;

END$$

DELIMITER ;

-- 6) Crear un rol llamado admin y otorgarle los siguientes permisos:
-- ● crear registros en la tabla Customers.
-- ● actualizar solamente la columna Phone de Customers.

CREATE ROLE admin;

GRANT INSERT ON Customers to admin;
GRANT UPDATE(Phone) ON Customers to admin;



        