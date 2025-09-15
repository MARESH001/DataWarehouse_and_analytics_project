/*
Stored Procedure:
Load Bronze Layer (Source Bronze)
Script Purpose:
This stored procedure loads data into the 'bronze schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the BULK INSERT command to load data from csv Files to bronze tables.
Parameters:
None.
This stored procedure does not accept any parameters or return any values.
Usage Example:
EXEC bronze.load_bronze; */
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN 
    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;

    SET @batch_start_time = GETDATE();

    BEGIN TRY 
        -- CRM Customer Info
        PRINT 'Truncating table bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT 'Inserting data into bronze.crm_cust_info';
        SET @start_time = GETDATE();

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';

        -- CRM Product Info
        PRINT 'Truncating table bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT 'Inserting data into bronze.crm_prd_info';
        SET @start_time = GETDATE();

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';

        -- CRM Sales Details
        PRINT 'Truncating table bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT 'Inserting data into bronze.crm_sales_details';
        SET @start_time = GETDATE();

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';

        -- ERP Location
        PRINT 'Truncating table bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT 'Inserting data into bronze.erp_loc_a101';
        SET @start_time = GETDATE();

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';

        -- ERP Customer
        PRINT 'Truncating table bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT 'Inserting data into bronze.erp_cust_az12';
        SET @start_time = GETDATE();

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';

        -- ERP Product Category
        PRINT 'Truncating table bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT 'Inserting data into bronze.erp_px_cat_g1v2';
        SET @start_time = GETDATE();

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\HP\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0d0a',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' seconds';

        -- Final Batch Duration
        SET @batch_end_time = GETDATE();
        PRINT '===========================================================';
        PRINT 'Bronze Load Completed Successfully';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR(20)) + ' seconds';
        PRINT '===========================================================';

    END TRY
    BEGIN CATCH
        PRINT '===========================================================';
        PRINT 'Error occurred during Bronze Layer Load';
        PRINT '===========================================================';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'Error State: '  + CAST(ERROR_STATE() AS NVARCHAR(10));
    END CATCH
END;

GO

EXEC bronze.load_bronze
