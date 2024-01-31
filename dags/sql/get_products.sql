SELECT
    P.productid AS productid,
    C.categoryname AS categoryname,
    S.contactname AS suppliercontactname
FROM
    "Products" AS P
    LEFT JOIN "Categories" AS C
        ON C.categoryid = P.categoryid
    LEFT JOIN "Suppliers" AS S
              ON S.supplierid = P.supplierid
WHERE EXISTS (SELECT List.productid FROM "tmpProductsList" AS List WHERE List.productid = P.productid);
