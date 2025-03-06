# dlt_pipelines/gold_pipeline.py
import dlt
from pyspark.sql import functions as F

@dlt.table(comment="Pivoted transactions per product aggregated by total sales per day")
def transactions_per_product():
    # Read transactions from the Silver layer (jp_assessment.latam_lab_silver)
    df = spark.table("jp_assessment.latam_lab_silver.sales_transactions")
    # Convert dateTime to date for grouping
    df = df.withColumn("trans_date", F.to_date("dateTime"))
    # Pivot: for each day, sum totalPrice per product
    df_pivot = df.groupBy("trans_date").pivot("product").agg(F.sum("totalPrice").alias("total_sales"))
    return df_pivot

@dlt.table(comment="Detailed transactions enriched with customer info")
def transactions_details():
    # Read transactions and customer data from Silver layer
    df_trans = spark.table("jp_assessment.latam_lab_silver.sales_transactions")
    df_customers = spark.table("jp_assessment.latam_lab_silver.sales_customers")
    # Join on customerID to add customer details (name, address, email)
    df_details = df_trans.join(df_customers, "customerID", "left") \
        .select(
            "transactionID",
            "customerID",
            "dateTime",
            "product",
            "quantity",
            "totalPrice",
            "paymentMethod",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_name"),
            "address",
            "email_address"
        )
    return df_details
