--- check bronze model
SELECT
*
FROM bronze.crm_cust_info


--- check for duplicates
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


--- Check for unwanted spaces first name
--- Expectation: No results??
--- if the original value is not equal to the same value after trimming it means there are spaces
--- can perform this for all string columns
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Check for unwanted spaces last name
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data standardisation & consistency
SELECT DISTINCT cst_gndr
from bronze.crm_cust_info



--- check silver model

SELECT
*
FROM silver.crm_cust_info


--- check for duplicates
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


--- Check for unwanted spaces first name
--- Expectation: No results??
--- if the original value is not equal to the same value after trimming it means there are spaces
--- can perform this for all string columns
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Check for unwanted spaces last name
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data standardisation & consistency
SELECT DISTINCT cst_gndr
from silver.crm_cust_info
