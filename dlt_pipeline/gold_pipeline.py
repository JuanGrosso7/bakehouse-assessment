import dlt
from pyspark.sql import functions as F

@dlt.table(comment="Pivoted transactions per product aggregated by total sales per day")
def transactions_per_product():
    df = spark.table("jp_assessment.latam_lab_silver.sales_transactions")
    df = df.withColumn("trans_date", F.to_date("dateTime"))
    df = df.withColumn("product_sanitized", F.regexp_replace("product", " ", "_"))
    df_pivot = df.groupBy("trans_date").pivot("product_sanitized").agg(F.sum("totalPrice").alias("total_sales"))
    return df_pivot

@dlt.table(comment="Detailed transactions enriched with customer info")
def transactions_details():
    df_trans = spark.table("jp_assessment.latam_lab_silver.sales_transactions")
    df_customers = spark.table("jp_assessment.latam_lab_silver.sales_customers")
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
