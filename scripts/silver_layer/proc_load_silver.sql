CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME, 
            @end_time DATETIME, 
            @table_start DATETIME, 
            @table_end DATETIME, 
            @table_name NVARCHAR(200);

    SET @start_time = GETDATE();
    PRINT '==== Silver Layer Load Started at ' + CONVERT(VARCHAR, @start_time, 120) + ' ====';

    BEGIN TRY
        -------------------------------------------------------------------
        -- CRM Customer Info
        -------------------------------------------------------------------
        SET @table_name = 'silver.crm_cust_info';
        SET @table_start = GETDATE();
        PRINT 'Loading ' + @table_name + '...';

        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            cst_id, cst_key,
            TRIM(cst_firstname), TRIM(cst_lastname),
            CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                 ELSE 'n/a' END,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
                 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
                 ELSE 'n/a' END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) t
        WHERE flag_last = 1;

        SET @table_end = GETDATE();
        PRINT 'Loaded ' + @table_name + ' in ' + CAST(DATEDIFF(SECOND, @table_start, @table_end) AS VARCHAR) + ' sec';

        -------------------------------------------------------------------
        -- CRM Product Info
        -------------------------------------------------------------------
        SET @table_name = 'silver.crm_prd_info';
        SET @table_start = GETDATE();
        PRINT 'Loading ' + @table_name + '...';

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost,
            prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            SUBSTRING(prd_key, 1, 5),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                 ELSE 'n/a' END,
            prd_start_dt,
            DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
        FROM bronze.crm_prd_info;

        SET @table_end = GETDATE();
        PRINT 'Loaded ' + @table_name + ' in ' + CAST(DATEDIFF(SECOND, @table_start, @table_end) AS VARCHAR) + ' sec';

        -------------------------------------------------------------------
        -- ERP Location
        -------------------------------------------------------------------
        SET @table_name = 'silver.erp_loc_a101';
        SET @table_start = GETDATE();
        PRINT 'Loading ' + @table_name + '...';

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
                 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                 ELSE TRIM(cntry) END
        FROM bronze.erp_loc_a101;

        SET @table_end = GETDATE();
        PRINT 'Loaded ' + @table_name + ' in ' + CAST(DATEDIFF(SECOND, @table_start, @table_end) AS VARCHAR) + ' sec';

        -------------------------------------------------------------------
        -- CRM Sales Details
        -------------------------------------------------------------------
        SET @table_name = 'silver.crm_sales_details';
        SET @table_start = GETDATE();
        PRINT 'Loading ' + @table_name + '...';

        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT 
            sls_ord_num, sls_prd_key, sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE TRY_CONVERT(DATE, CAST(sls_order_dt AS CHAR(8))) END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                 ELSE TRY_CONVERT(DATE, CAST(sls_ship_dt AS CHAR(8))) END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                 ELSE TRY_CONVERT(DATE, CAST(sls_due_dt AS CHAR(8))) END,
            CASE WHEN sls_sales IS NULL OR sls_sales < 0 OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                 THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sls_price END
        FROM bronze.crm_sales_details;

        SET @table_end = GETDATE();
        PRINT 'Loaded ' + @table_name + ' in ' + CAST(DATEDIFF(SECOND, @table_start, @table_end) AS VARCHAR) + ' sec';

        -------------------------------------------------------------------
        -- ERP PX Cat G1V2
        -------------------------------------------------------------------
        SET @table_name = 'silver.erp_px_cat_g1v2';
        SET @table_start = GETDATE();
        PRINT 'Loading ' + @table_name + '...';

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @table_end = GETDATE();
        PRINT 'Loaded ' + @table_name + ' in ' + CAST(DATEDIFF(SECOND, @table_start, @table_end) AS VARCHAR) + ' sec';

    END TRY
    BEGIN CATCH
        PRINT 'Error loading ' + ISNULL(@table_name, 'Unknown table');
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        RETURN;
    END CATCH;

    SET @end_time = GETDATE();
    PRINT '==== Silver Layer Load Completed at ' + CONVERT(VARCHAR, @end_time, 120) + ' ====';
    PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' sec';

END;
GO

EXEC silver.load_silver
