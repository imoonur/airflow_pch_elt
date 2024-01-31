# PCh ELT

PCh ELT pipeline.

## Overview
In this example, we leverage Apache Airflow to automate the following steps:

1. Primary data load: fetch data from primary source and put them into s3.
2. Put ID's from s3 to secondary sources. 
3. Secondary data load: fetch data from secondary sources using specified ID's.
4. Results Aggregation: aggregating loaded data.
5. Data cleaning.
6. Feature generation.

Second point tasks must be executed in parallel.

Final dataset must contain these variables:

- orderid +
- orderdate +
- shipcountry +
- totalcost   +               -- unitprice * qty * discount
- topproductcategoryname  +       -- "Seafood"
- topproductsuppliercontactname +- shipperphone +
- custcompanyname +
- custcontactname +
- custcity +
- empfirstname +
- empbirthdate +
- emphiredate +








