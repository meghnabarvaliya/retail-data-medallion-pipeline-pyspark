# retail-data-medallion-pipeline-pyspark
End-to-end PySpark ETL pipeline implementing the Medallion architecture (Bronze → Silver → Gold) for retail order data. Built and executed in Google Colab using data from Google Drive.

🚀 Overview

This project demonstrates a real-world end-to-end Data Engineering pipeline built using Apache PySpark, following the Medallion Architecture (Bronze → Silver → Gold).

The goal is to ingest raw retail datasets, clean and transform them, and generate actionable business insights such as top customers, state-level revenue, monthly trends, and product sales.



| Layer         | Description                          | Example Output                      |
| ------------- | ------------------------------------ | ----------------------------------- |
| 🥉 **Bronze** | Raw, messy data exactly as received. | Raw CSVs from retail system         |
| 🥈 **Silver** | Cleaned, typed, validated data.      | Fixed dates, IDs, null handling     |
| 🥇 **Gold**   | Aggregated business insights.        | Monthly revenue, top customers, AOV |


<img width="313" height="541" alt="image" src="https://github.com/user-attachments/assets/5c5bc123-51b3-46de-a81e-ffcb679fbd81" />



| Dataset             | Description      | Key Columns                                                                                        |
| ------------------- | ---------------- | -------------------------------------------------------------------------------------------------- |
| **orders.csv**      | Order metadata   | `order_id`, `order_date`, `order_customer_id`, `order_status`                                      |
| **customers.csv**   | Customer details | `customer_id`, `customer_fname`, `customer_city`, `customer_state`, `customer_email`               |
| **order_items.csv** | Order line items | `order_item_id`, `order_id`, `order_item_product_id`, `order_item_quantity`, `order_item_subtotal` |




| Issue                    | Cleaning Step                                      | Function Used                  |
| ------------------------ | -------------------------------------------------- | ------------------------------ |
| Mixed date formats       | Unified using multiple patterns                    | `to_date()` + `coalesce()`     |
| Null or bad customer IDs | Cleaned and casted to `IntegerType`                | `regexp_replace()` + `.cast()` |
| Null emails or cities    | Replaced with `"unknown@domain.com"` / `"Unknown"` | `when()` + `lit()`             |
| Null quantity/price      | Defaulted and recalculated subtotal                | `when()` + arithmetic          |
| Added DQ flag            | To track missing fields                            | `dq_flag` column               |





| #   | Business Question                   | Logic                                                            |
| --- | ----------------------------------- | ---------------------------------------------------------------- |
| 1️⃣ | **Top 10 Customers by Total Spend** | Sum of `order_item_subtotal` per customer                        |
| 2️⃣ | **Revenue per State**               | Join customers → orders → order_items; group by `customer_state` |
| 3️⃣ | **Monthly Revenue Trend**           | Extract month/year from `order_date`, sum revenue                |
| 4️⃣ | **Average Order Value (AOV)**       | Total revenue ÷ total number of orders                           |
| 5️⃣ | **Most Sold Products**              | Sum of `order_item_quantity` per `order_item_product_id`         |
| 6️⃣ | **Orders Missing Customer Info**    | Filter for `null` or `Unknown` customer details                  |



🧑‍💻 Author

Meghna Barvaliya
Data Engineer | Spark | Python | SQL | Databricks | Azure | Data Modeling
