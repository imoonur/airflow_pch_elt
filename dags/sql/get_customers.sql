SELECT
    C.custid,
    C.companyname,
    C.contactname,
    C.city
FROM  "Customers" AS C
WHERE EXISTS (SELECT List.custid FROM "tmpCustomersList" AS List WHERE List.custid = C.custid);
