-- This script performs the full ETL pipeline for 2021–2023 beverage order data
-- including importing, transforming, consolidating, and summarizing records
-- for analysis of product sales under specific Vice Presidents.

-- --------------------------------------------------------------------------------
-- STEP 0: Create and Populate Beverages Database Using Provided Script
-- This step executes the Category_and_Org_table.SQL file provided in the assignment.
-- It sets up the "beverages" database, and creates and populates the Category
-- and Org_Chart_Table. These are assumed accurate and required for subsequent steps.
-- --------------------------------------------------------------------------------

-- Run the provided Category_and_Org_table.SQL script before proceeding.
-- It includes:
--   - DROP DATABASE IF EXISTS beverages;
--   - CREATE DATABASE beverages;
--   - USE beverages;
--   - CREATE TABLE Category (...);
--   - CREATE TABLE Org_Chart_Table (...);
--   - INSERT INTO Category ...;
--   - INSERT INTO Org_Chart_Table ...;

-- After running that script, continue with the following ETL steps.

-- --------------------------------------------------------------------------------
-- STEP 1: Import Raw Order Data Tables Using Import Wizard
-- These files were provided as .csv extracts from historical order databases
-- and are assumed to contain beverage order transactions from the years 2021 to 2023.
-- The import wizard was used to load these flat files into the "beverages" database schema
-- under their original names, which included hyphens.
-- These tables were then renamed using SQL to match ETL naming standards.
-- --------------------------------------------------------------------------------

-- Original table imports:
--   beverage-orders-2021.csv → `beverage-orders-2021`
--   beverage-orders-2022.csv → `beverage-orders-2022`
--   beverage-orders-2023.csv → `beverage-orders-2023`

-- Renamed tables to follow ETL convention using the following SQL commands:
RENAME TABLE `beverage-orders-2021` TO bev_orders_2021;
RENAME TABLE `beverage-orders-2022` TO bev_orders_2022;
RENAME TABLE `beverage-orders-2023` TO bev_orders_2023;

-- Final staging tables in use: bev_orders_2021, bev_orders_2022, bev_orders_2023

-- --------------------------------------------------------------------------------
-- STEP 2: Create the Consolidated Interim Table
-- This table will serve as a harmonized structure into which all filtered and transformed data
-- from the three staging tables will be inserted. All numeric metrics are preserved, and
-- three new calculated fields are created based on multiplication of existing values:
--   Volume_Quantity = Volume * Quantity
--   Weight_Quantity = Weight * Quantity
--   Revenue_Quantity = Per_Unit_Price * Quantity
-- Additional fields include Category and Vice President (First and Last Name).
-- --------------------------------------------------------------------------------

DROP TABLE IF EXISTS consolidated_beverage_data;
CREATE TABLE consolidated_beverage_data (
    Product VARCHAR(50),
    Weight INT,
    Volume INT,
    Caffeine INT,
    Per_Unit_Price INT,
    Quantity INT,
    Volume_Quantity INT,
    Weight_Quantity INT,
    Revenue_Quantity INT,
    Region VARCHAR(50),
    State VARCHAR(50),
    Country VARCHAR(50),
    Category VARCHAR(50),
    First_Name VARCHAR(50),
    Last_Name VARCHAR(50),
    Year INT
);

-- --------------------------------------------------------------------------------
-- STEP 3: Insert Filtered & Transformed Data into Consolidated Table
-- Business Rule 1: Only include data managed by the following Vice Presidents:
--   Remi Olson, Buford Jackson, Bodhi Perry, and Rowan Walsh
-- Business Rule 2: Use the Category table to map products to categories
-- Business Rule 3: Use the Org_Chart_Table to map categories to the correct VP
-- Business Rule 4: Compute Volume_Quantity, Weight_Quantity, Revenue_Quantity as:
--     Volume_Quantity = Volume * Quantity
--     Weight_Quantity = Weight * Quantity
--     Revenue_Quantity = Per_Unit_Price * Quantity
-- Business Rule 5: Handle missing Caffeine data in 2021 by inserting NULL
-- --------------------------------------------------------------------------------

-- Insert 2021 order data (Caffeine column missing, substituted with NULL)
-- Import all qualifying records from bev_orders_2021 into consolidated_beverage_data
-- after joining on Category and Org Chart to map to valid Vice Presidents
INSERT INTO consolidated_beverage_data
SELECT 
    c.Product_name,
    b.Weight,
    b.Volume,
    NULL AS Caffeine,
    b.Per_Unit_Price,
    b.Quantity,
    b.Volume * b.Quantity AS Volume_Quantity,
    b.Weight * b.Quantity AS Weight_Quantity,
    b.Per_Unit_Price * b.Quantity AS Revenue_Quantity,
    b.Region,
    b.State,
    b.Country,
    c.Category_Name,
    o.First_Name,
    o.Last_Name,
    2021 AS Year
FROM bev_orders_2021 b
JOIN Category c ON b.Product = c.Product_name
JOIN Org_Chart_Table o ON c.Category_Name = o.Category_Name
WHERE o.First_Name IN ('Remi', 'Buford', 'Bodhi', 'Rowan');

-- Insert 2022 order data
-- Standard insert with direct reference to Caffeine field
INSERT INTO consolidated_beverage_data
SELECT 
    c.Product_name,
    b.Weight,
    b.Volume,
    b.Caffeine,
    b.Per_Unit_Price,
    b.Quantity,
    b.Volume * b.Quantity AS Volume_Quantity,
    b.Weight * b.Quantity AS Weight_Quantity,
    b.Per_Unit_Price * b.Quantity AS Revenue_Quantity,
    b.Region,
    b.State,
    b.Country,
    c.Category_Name,
    o.First_Name,
    o.Last_Name,
    2022 AS Year
FROM bev_orders_2022 b
JOIN Category c ON b.Product = c.Product_name
JOIN Org_Chart_Table o ON c.Category_Name = o.Category_Name
WHERE o.First_Name IN ('Remi', 'Buford', 'Bodhi', 'Rowan');

-- Insert 2023 order data
-- Standard insert with direct reference to Caffeine field
INSERT INTO consolidated_beverage_data
SELECT 
    c.Product_name,
    b.Weight,
    b.Volume,
    b.Caffeine,
    b.Per_Unit_Price,
    b.Quantity,
    b.Volume * b.Quantity AS Volume_Quantity,
    b.Weight * b.Quantity AS Weight_Quantity,
    b.Per_Unit_Price * b.Quantity AS Revenue_Quantity,
    b.Region,
    b.State,
    b.Country,
    c.Category_Name,
    o.First_Name,
    o.Last_Name,
    2023 AS Year
FROM bev_orders_2023 b
JOIN Category c ON b.Product = c.Product_name
JOIN Org_Chart_Table o ON c.Category_Name = o.Category_Name
WHERE o.First_Name IN ('Remi', 'Buford', 'Bodhi', 'Rowan');

-- --------------------------------------------------------------------------------
-- STEP 4: Create Final Summary Table for Output
-- Final aggregation required by business for analysis includes:
-- - Summing Quantity and Revenue_Quantity fields
-- - Maintaining base product attributes such as weight, volume, caffeine, price
-- - Grouped by Year, VP, Category, Product, Region, State, Country
-- Sorting business rules:
--   1. Alphabetically by VP Last Name, then First Name
--   2. Year ascending (2021 → 2023)
--   3. Alphabetically by Category and Product
--   4. Alphabetically by Country and Region
--   5. Revenue descending (within all other groupings)
-- --------------------------------------------------------------------------------

DROP TABLE IF EXISTS final_beverage_summary;
CREATE TABLE final_beverage_summary AS
SELECT
    o.Last_Name AS VP_Last_Name,
    o.First_Name AS VP_First_Name,
    c.Year,
    c.Category,
    c.Product,
    c.Country,
    c.Region,
    c.State,
    c.Weight,
    c.Volume,
    c.Caffeine,
    c.Per_Unit_Price,
    SUM(c.Quantity) AS Quantity_Sum,
    SUM(c.Revenue_Quantity) AS Revenue_Quantity_Sum
FROM consolidated_beverage_data c
JOIN Org_Chart_Table o ON c.First_Name = o.First_Name AND c.Last_Name = o.Last_Name
GROUP BY
    o.Last_Name, o.First_Name, c.Year, c.Category, c.Product,
    c.Country, c.Region, c.State, c.Weight, c.Volume,
    c.Caffeine, c.Per_Unit_Price
ORDER BY
    o.Last_Name ASC,
    o.First_Name ASC,
    c.Year ASC,
    c.Category ASC,
    c.Product ASC,
    c.Country ASC,
    c.Region ASC,
    Revenue_Quantity_Sum DESC;

-- --------------------------------------------------------------------------------
-- STEP 5: Exporting Results
-- Export both final tables into .csv format using the Table Export Wizard:
--   1. consolidated_beverage_data → G1_consolidated_beverage_data.csv
--   2. final_beverage_summary → G1_output_final.csv
-- These files will be submitted as deliverables for the assignment.
-- --------------------------------------------------------------------------------
-- Exported consolidated_beverage_data to G1_consolidated_beverage_data.csv
-- Exported final_beverage_summary to G1_output_final.csv
