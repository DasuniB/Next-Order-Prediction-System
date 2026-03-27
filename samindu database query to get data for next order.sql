SHOW databases;
use samindu_db;
SELECT 
Date as date,
InvoiceNo as invoiceno,
CustomerCode as customercode,
CustomerName as customername,
ProductCode as productcode,
ProductName as productname,
Qty as qty,
RepCode as repcode 
FROM sales_flat where Date>='2025-06-01' and Date<='2025-12-01';


USE order_prediction_db;
SHOW TABLES;
SELECT * FROM suggested_order;
SELECT * FROM suggested_order where repid="2057"
SHOW databases;

USE order_prediction_db;
SHOW TABLES;
SELECT * FROM suggested_order;
DESC suggested_order;