SELECT
    orderid,
    custid,
    empid,
    orderdate,
    shipperid,
    shipcountry
FROM dbo.Orders
WHERE orderdate >= '20160501';
