DROP TABLE IF EXISTS "Data";
CREATE TABLE "Data" (
    orderid int NOT NULL PRIMARY KEY,
    orderdate date,
    shipcountry varchar(15),
    totalcost numeric(38),
    topproductcategoryname varchar(15),
    topproductsuppliercontactname varchar(30),
    shipperphone char(24),
    custcompanyname varchar(40),
    custcontactname varchar(30),
    custcity varchar(15),
    empfirstname char(10),
    empbirthdate date,
    emphiredate date);
