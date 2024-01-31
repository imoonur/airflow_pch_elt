SELECT
    OD.orderid,
    OD.productid,
    OD.unitprice,
    OD.qty,
    OD.discount
FROM dbo.Orderdetails AS OD
WHERE EXISTS (SELECT List.orderid FROM ##OrderList AS List WHERE List.orderid = OD.orderid);
