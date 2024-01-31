WITH TmpTable AS (
    SELECT *
    FROM (VALUES
              (1, 'a'),
              (2, 'b'),
              (3, 'c')) AS T(n, s))
SELECT *
INTO ##TestTempTable
FROM TmpTable;