/*
==========================================================================
Stored Procedure: Load Silver Layer (Bronze > Silver)
==========================================================================
Script Purpose:
        This stored procedure loads data into the 'silver' schema from the 'bronze' layer
        It performs the following actions:
        - Transforms the bronze tables using a combination of data entrichment, standardisation, and data clean-up
        - Truncates the silver tables before loading the data
        - Uses the 'INSERT INTO' command to load the data from the broze to silver tables
The process above can also be referred to as the ETL process (Extract, Transform, Load)

Parameters:
        None
                This stored procedure does not accept any parameters or return any values

Usage Example:
        EXEC silver.load_silver;
==========================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
        BEGIN TRY
                SET @batch_start_time = GETDATE();
                PRINT '===================================================';
                PRINT 'Loading Silver Layer';
                PRINT '===================================================';    

                        PRINT '---------------------------------------------------';
                        PRINT 'Loading CRM Tables';
                        PRINT '---------------------------------------------------';

                        -- Loading silver.crm_cust_info
                        SET @start_time  = GETDATE();
                                PRINT 'Truncating Table: silver.crm_cust_info'
                                TRUNCATE TABLE silver.crm_cust_info;

                                PRINT '>> Insterting Data Into: silver.crm_cust_info'
                                INSERT INTO silver.crm_cust_info (
                                cst_id,
                                cst_key,
                                cst_firstname,
                                cst_lastname,
                                cst_marital_status,
                                cst_gndr,
                                cst_create_date)

                                SELECT
                                cst_id,
                                cst_key,
                                TRIM(cst_firstname) AS cst_firstname,
                                TRIM(cst_lastname) AS cst_lastname,
                                CASE    WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                                        WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                                        ELSE 'Unknown'
                                END AS cst_marital_status, -- Normalise marital status values to readable format

                                CASE    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                                        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                                        ELSE 'Unknown'
                                END AS cst_gndr, -- Normalise gender values to readable format

                                cst_create_date 
                                FROM (
                                SELECT
                                *,
                                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
                                FROM bronze.crm_cust_info
                                WHERE cst_id IS NOT NULL
                                --WHERE cst_id = 29466
                                )t 
                                WHERE flag_last = 1 -- Select the most recent data entry/record per customer
                        SET @end_time = GETDATE();
                        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
                        PRINT '>> ----------------';

                        -- Loading silver.crm_prd_info
                        SET @start_time = GETDATE();
                                PRINT '>> Truncating Table: silver.crm_prd_info'
                                TRUNCATE TABLE silver.crm_prd_info;

                                PRINT '>> Inserting Data Into: silver.crm_prd_info'
                                INSERT INTO silver.crm_prd_info (
                                prd_id,
                                cat_id,
                                prd_key,
                                prd_nm,
                                prd_cost,
                                prd_line,
                                prd_start_dt,
                                prd_end_dt
                                )

                                SELECT
                                prd_id,
                                replace(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- created new column based on existing
                                SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,  -- created new column based on existing
                                prd_nm,
                                ISNULL(prd_cost,0) as prd_cost,
                                CASE    UPPER(TRIM(prd_line))   -- quick case when for non complex mappings
                                        WHEN 'M' THEN 'Mountain'
                                        WHEN 'R' THEN 'Road'
                                        WHEN 'T' THEN 'Touring'
                                        WHEN 'M' THEN 'Other Sales'
                                        ELSE 'n/a'
                                END AS prd_line,
                                CAST(prd_start_dt AS DATE) AS prd_start_dt,     -- data type casting
                                CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- cast and enrichment
                                FROM bronze.crm_prd_info
                        SET @end_time = GETDATE();
                        PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
                        PRINT '-------------------';

                        -- Loading silver.crm_sales_details
                        SET @start_time = GETDATE();
                                PRINT '>> Truncating Table: silver.crm_sales_details'
                                TRUNCATE TABLE silver.crm_sales_details;

                                PRINT '>> Insterting Data Into: silver.crm_sales_details'
                                INSERT INTO silver.crm_sales_details (
                                        sls_ord_num,
                                        sls_prd_key,
                                        sls_cust_id,
                                        sls_order_dt,
                                        sls_ship_dt,
                                        sls_due_dt,
                                        sls_sales,
                                        sls_quantity,
                                        sls_price
                                )

                                SELECT
                                sls_ord_num,
                                sls_prd_key,
                                sls_cust_id,
                                CASE    WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
                                        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
                                END AS sls_order_dt,
                                CASE    WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
                                        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
                                END AS sls_ship_dt,
                                CASE    WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
                                        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
                                END AS sls_due_dt,
                                CASE    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != (sls_quantity) * ABS(sls_price)
                                        THEN (sls_quantity) * ABS(sls_price)
                                        ELSE sls_sales
                                END AS sls_sales,
                                sls_quantity,
                                CASE    WHEN sls_price IS NULL OR sls_price <= 0
                                        THEN sls_sales / NULLIF(sls_quantity, 0)
                                        ELSE sls_price
                                END AS sls_price
                                FROM bronze.crm_sales_details
                        SET @end_time = GETDATE();
                        PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
                        PRINT '-------------------';

                        PRINT '---------------------------------------------------';
                        PRINT 'Loading ERP Tables';
                        PRINT '---------------------------------------------------';

                        -- Loading silver.erp_cust_az12
                        SET @start_time = GETDATE();
                                PRINT '>> Truncating Table: silver.erp_cust_az12'
                                TRUNCATE TABLE silver.erp_cust_az12;

                                PRINT '>> Inserting Data Into: silver.erp_cust_az12'
                                INSERT INTO silver.erp_cust_az12 (
                                        cid,
                                        bdate,
                                        gen
                                )

                                SELECT
                                CASE  WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                                ELSE cid
                                END cid,
                                CASE WHEN bdate > GETDATE() THEN NULL
                                ELSE bdate
                                END AS bdate,
                                CASE  WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('F', 'FEMALE') THEN 'Female' -- data standardisation (remove new line, extra spaces etc)
                                WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('M', 'MALE') THEN 'Male'
                                ELSE 'n/a'
                                END AS gen
                                FROM bronze.erp_cust_az12
                        SET @end_time = GETDATE();
                        PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
                        PRINT '-------------------';

                        -- Loading silver.erp_loc_a101
                        SET @start_time = GETDATE();
                                PRINT '>> Truncating Table: silver.erp_loc_a101'
                                TRUNCATE TABLE silver.erp_loc_a101;

                                PRINT '>> Inserting Data Into: silver.erp_loc_a101'
                                INSERT INTO silver.erp_loc_a101
                                (cid, cntry)

                                SELECT
                                REPLACE (cid, '-', '') AS cid,
                                CASE  WHEN UPPER(TRIM(REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))) = 'DE' THEN 'Germany'
                                WHEN UPPER(TRIM(REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))) IN ('US', 'USA') THEN 'United States'
                                WHEN UPPER(TRIM(REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))) = '' OR cntry IS NULL THEN 'n/a'
                                ELSE (REPLACE(REPLACE (cntry, CHAR(10), ''), CHAR(13), ''))
                                END AS cntry
                                from bronze.erp_loc_a101
                        SET @end_time = GETDATE();
                        PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
                        PRINT '-------------------';

                        -- Loading silver.erp_px_cat_g1v2
                        SET @start_time = GETDATE();
                                PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
                                TRUNCATE TABLE silver.erp_px_cat_g1v2;

                                PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
                                INSERT INTO silver.erp_px_cat_g1v2 (
                                        id,
                                        cat,
                                        subcat,
                                        maintenance
                                )

                                SELECT
                                id,
                                cat,
                                subcat,
                                REPLACE(REPLACE(maintenance, CHAR(10), ''), CHAR(13), '') AS maintenance
                                FROM bronze.erp_px_cat_g1v2
                        SET @end_time = GETDATE();
                        PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
                        PRINT '-------------------';
                SET @batch_end_time = GETDATE();
                PRINT '===================================================';
                PRINT 'Loading Silver Layer Complete';
                PRINT '>> Total Load Duration: ' +CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
                PRINT '===================================================';    
        END TRY
        BEGIN CATCH
                PRINT '===================================================';
                PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
                PRINT 'Error Message' + ERROR_MESSAGE();
                PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
                PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
                PRINT '===================================================';
        END CATCH
END
