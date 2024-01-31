SELECT
    E.empid,
    E.firstname,
    E.birthdate,
    E.hiredate
FROM  "Employees" AS E
WHERE EXISTS (SELECT List.empid FROM "tmpEmployeesList" AS List WHERE List.empid = E.empid);
