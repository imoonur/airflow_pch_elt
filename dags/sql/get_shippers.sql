SELECT
    S.shipperid,
    S.phone
FROM  "Shippers" AS S
WHERE EXISTS (SELECT List.shipperid FROM "tmpShippersList" AS List WHERE List.shipperid = S.shipperid);
