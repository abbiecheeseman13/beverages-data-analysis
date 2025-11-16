# Beverages-R-Us: SQL ETL and Data Visualization Project

This project demonstrates an end-to-end SQL workflow using three years of beverage order data (2021–2023). The goal was to build a complete ETL pipeline, clean and standardize raw data, apply business rules, and generate a final summary dataset suitable for analysis. I also created supporting visualizations to explore revenue trends across categories, regions, and vice presidents.

This project highlights my practical SQL skills as an early-career data analyst and shows how I work through a structured data preparation and analysis process.

---

## Project Overview

Beverages-R-Us provided:

- Three annual order extracts (CSV format)
- A product category lookup table
- An organizational chart mapping categories to vice presidents

My objectives were to:

1. Consolidate raw files into a clean, standardized database  
2. Join products to their categories and assigned vice presidents  
3. Build calculated fields such as quantity-based metrics and revenue  
4. Apply business rules to filter the dataset  
5. Create a final aggregated summary table  
6. Build visualizations to support business insights  

The complete ETL process is documented in the SQL script.

---

## Tools Used

- SQL (MySQL Workbench)
- Excel and CSV exports
- Tableau or Power BI for visualization

---

## Visual Summary

The analysis is captured through several visualizations, including:

- Revenue by state across three years  
- Revenue by product category  
- Revenue by region and vice president  
- Annual revenue by vice president  
- Annual trends by beverage category  

A dashboard image is included in the repository.

---

## Database Structure

### Raw Input Tables  
- `bev_orders_2021`  
- `bev_orders_2022`  
- `bev_orders_2023`  

### Reference Tables  
- `Category`  
- `Org_Chart_Table`  

### Output Tables  
- `consolidated_beverage_data`  
- `final_beverage_summary`  

---

## ETL Workflow

### 1. Extract  
Imported all raw order files into MySQL Workbench. File names and field structures differed by year, including a missing caffeine column in the 2021 data.

### 2. Transform  
Key transformation steps included:

- Renaming imported tables to match naming conventions  
- Joining product data with the Category table  
- Joining category data with the Org Chart table to assign vice presidents  
- Filtering data to the four required vice presidents  
- Creating calculated fields:  
  - Volume multiplied by quantity  
  - Weight multiplied by quantity  
  - Revenue as price multiplied by quantity  
- Handling missing caffeine data in 2021 by using null values  

### 3. Load  
Cleaned records from all years were combined into a single table, `consolidated_beverage_data`.  
A second table, `final_beverage_summary`, aggregates revenue and quantity by year, vice president, category, product, and region.

---

## Key Insights

- Vitamin Drinks and Iced Teas generated the highest revenue overall.  
- The South and Midwest were consistently strong regions for sales.  
- 2022 produced the highest total revenue across most product categories.  
- Hot Beverages and Iced Teas showed clear year-over-year growth.  
- Several categories, including Smoothies and Mocktails, declined in 2023.

---

## Skills Demonstrated

- SQL data cleaning and transformation  
- Multi-table joins and lookup integration  
- Calculated fields and business rule implementation  
- Data modeling and table consolidation  
- Aggregation, grouping, and KPI creation  
- Turning raw data into clear analytical outputs  

---

## How to Reproduce

1. Clone the repository.  
2. Run the setup script that contains the Category and Org Chart tables.  
3. Run the main ETL SQL script in MySQL Workbench.  
4. Export the summary tables as CSV files.  
5. Load the final summary data into Tableau or Power BI to recreate the dashboard.

---

## Repository Contents

| File | Description |
|------|-------------|
| `G1_Script1.sql` | Full SQL ETL script, including extraction, transformation, and loading |
| `Beverages-R-Us-Visualization.jpg` | Dashboard with final visual analysis |
| `G1_consolidated_beverage_data.csv` | Consolidated dataset after cleaning |
| `G1_output_final.csv` | Final grouped summary table |

---

## Future Enhancements

- Build an interactive Tableau Public dashboard  
- Add Python scripts for exploratory data analysis  
- Create automated data validation checks  
- Scale the project using a cloud warehouse such as Snowflake or BigQuery
