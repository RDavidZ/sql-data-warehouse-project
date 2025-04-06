SELECT TOP (1000) [prd_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [DataWarehouse].[bronze].[crm_prd_info]

  ---Check for nulls
  --- Desired result is none
  
  SELECT
  prd_id,
  COUNT(*)
  FROM bronze.crm_prd_info
  GROUP BY prd_id
  HAVING COUNT(*) > 1 OR prd_id IS NULL




  SELECT
  prd_id,
  prd_key,
  SUBSTRING(prd_key, 1, 5) AS cat_id,
  prd_nm,
  prd_cost
  prd_line,
  prd_start_dt,
  prd_end_dt
  FROM bronze.crm_prd_info

  --- Check if we can join to erp table by cat id

  SELECT distinct id from bronze.erp_px_cat_g1v2

  --- replace - with _ to match ids
  SELECT
  prd_id,
  prd_key,
  replace(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  prd_nm,
  prd_cost
  prd_line,
  prd_start_dt,
  prd_end_dt
  FROM bronze.crm_prd_info

  -- check that they match (or not)
  --- check which cat_ids in the prd_info are not in the erp table
  SELECT
  prd_id,
  prd_key,
  replace(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  prd_nm,
  prd_cost
  prd_line,
  prd_start_dt,
  prd_end_dt
  FROM bronze.crm_prd_info
  WHERE replace(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
    (SELECT distinct id from bronze.erp_px_cat_g1v2)

-- transform to get the prd_key to map to sales table
  SELECT
  prd_id,
  prd_key,
  replace(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
  prd_nm,
  prd_cost
  prd_line,
  prd_start_dt,
  prd_end_dt
  FROM bronze.crm_prd_info

  -- sales table
  SELECT sls_prd_key FROM bronze.crm_sales_details

-- check that they match (the ids)
  SELECT
  prd_id,
  prd_key,
  replace(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
  prd_nm,
  prd_cost
  prd_line,
  prd_start_dt,
  prd_end_dt
  FROM bronze.crm_prd_info
  WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN
 (SELECT sls_prd_key FROM bronze.crm_sales_details)

 -- check for unwanted spaces in prd_nm
 -- desired: no results
 SELECT prd_nm
 FROM bronze.crm_prd_info
 WHERE prd_nm != TRIM(prd_nm)

 -- check quality of numbers in cost column
 -- desired: no result
 SELECT prd_cost
 from bronze.crm_prd_info
 WHERE prd_cost < 0 OR prd_cost IS NULL

-- repace nulls
  SELECT
  prd_id,
  prd_key,
  replace(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
  prd_nm,
  ISNULL(prd_cost,0) as prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt
  FROM bronze.crm_prd_info

-- data standardisation and consistency
SELECT DISTINCT prd_line
from bronze.crm_prd_info

-- quality of the dates
-- some start dates are after end date
-- if dates are switched around, some data will be overlapping for product price as an example
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
where prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

/*
check quality of silver prd table
*/
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

 SELECT prd_cost
 from silver.crm_prd_info
 WHERE prd_cost < 0 OR prd_cost IS NULL

SELECT prd_nm
 FROM silver.crm_prd_info
 WHERE prd_nm != TRIM(prd_nm)

 SELECT DISTINCT prd_line
from silver.crm_prd_info


-- desired result: no result
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
where sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

-- check quality of date columns
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

SELECT
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101

SELECT
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101


--- check for invalid order dates
SeLECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- sales must be equal to quantity * price
-- no negative, null or zero values
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_quantity) * ABS(sls_price)
            THEN ABS(sls_quantity) * ABS(sls_price)
        ELSE sls_sales
END AS sls_sales,

CASE    WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
/*WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_price*/

-- check quality of date columns
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

SELECT
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101

SELECT
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101


--- check for invalid order dates
SeLECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- sales must be equal to quantity * price
-- no negative, null or zero values
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_price


SELECT
CASE  WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
      ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
      ELSE bdate
END AS bdate,
CASE  WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('F', 'FEMALE') THEN 'Female'
      WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('M', 'MALE') THEN 'Male'
      ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12
WHERE cid NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- identify out-of-range dates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()


-- Data standardisation & consistency
SELECT DISTINCT
gen,
-- TRIM(gen) AS gen
CASE  WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('F', 'FEMALE') THEN 'Female'
      WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('M', 'MALE') THEN 'Male'
      ELSE 'n/a'
END AS new_gen
FROM bronze.erp_cust_az12

-- check data
-- an issues with the cid (-)
SELECT
cid,
cntry
from bronze.erp_loc_a101

-- fix cid and cntry
SELECT
REPLACE (cid, '-', '') AS cid,
CASE  WHEN UPPER(TRIM(REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))) = 'DE' THEN 'Germany'
      WHEN UPPER(TRIM(REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))) IN ('US', 'USA') THEN 'United States'
      WHEN UPPER(TRIM(REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))) = '' OR cntry IS NULL THEN 'n/a'
      ELSE (REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))
END AS cntry
from bronze.erp_loc_a101

-- Data standardisation & consistency
SELECT DISTINCT 
cntry as old_cntry,
CASE  WHEN UPPER(TRIM(REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))) = 'DE' THEN 'Germany'
      WHEN UPPER(TRIM(REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))) IN ('US', 'USA') THEN 'United States'
      WHEN UPPER(TRIM(REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))) = '' OR cntry IS NULL THEN 'n/a'
      ELSE (REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry


-- check for missing values at cid
-- desired result - no entries
SELECT
REPLACE (cid, '-', '') AS cid,
REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), '') AS cntry
from bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info)


-- check if cat_id can be joined to id from prd_info and producte details tab le
SELECT
id
FROM bronze.erp_px_cat_g1v2 WHERE id  IN
(SELECT cat_id FROM silver.crm_prd_info)

-- check for unwanted spaces in prod_details tables
-- desired: no results
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- data standardisation and consistency
SELECT DISTINCT 
subcat
FROM bronze.erp_px_cat_g1v2



