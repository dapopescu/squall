# TPCH5:ver1.0

SELECT NATION.NAME, SUM(LINEITEM.EXTENDEDPRICE * (1.0 - LINEITEM.DISCOUNT))
FROM CUSTOMER inner join ORDERS on CUSTOMER.CUSTKEY = ORDERS.CUSTKEY
inner join SUPPLIER on CUSTOMER.NATIONKEY = SUPPLIER.NATIONKEY
inner join LINEITEM on LINEITEM.ORDERKEY = ORDERS.ORDERKEY AND LINEITEM.SUPPKEY = SUPPLIER.SUPPKEY
inner join NATION on SUPPLIER.NATIONKEY = NATION.NATIONKEY
inner join REGION on NATION.REGIONKEY = REGION.REGIONKEY
WHERE  REGION.NAME = 'ASIA' AND
ORDERS.ORDERDATE >= {d '1994-01-01'} AND ORDERS.ORDERDATE < {d '1995-01-01'}
GROUP BY NATION.NAME
