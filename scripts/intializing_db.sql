USE master;
GO

-- Drop the database if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
    ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Datawarehouse;
END;
GO

-- Create new database
CREATE DATABASE Datawarehouse;
GO

-- Switch to the new database
USE Datawarehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
