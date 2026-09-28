from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# 1. إنشاء جلسة Spark بدون إجبار Hive Metastore Client
spark = SparkSession.builder \
    .appName("Health Insurance Data Warehouse Star Schema") \
    .getOrCreate()

# 2. قراءة ملف الـ CSV الرئيسي من HDFS
csv_hdfs_path = "hdfs://localhost:9000/data/raw/health_data.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_hdfs_path)

# 3. تقسيم البيانات إلى 1 Fact Table و 2 Dimension Tables (Star Schema)
dim_customer = df.select("id", "Gender", "Age", "Driving_License", "Region_Code").distinct()

dim_vehicle = df.select("Vehicle_Age", "Vehicle_Damage").distinct() \
                .withColumn("vehicle_id", monotonically_increasing_id())

fact_policy = df.join(dim_vehicle, on=["Vehicle_Age", "Vehicle_Damage"], how="inner") \
                .select("id", "vehicle_id", "Annual_Premium", "Policy_Sales_Channel", "Vintage") \
                .withColumnRenamed("id", "customer_id")

# 4. حفظ الجداول بصيغة Parquet على HDFS (Data Warehouse Layer)
dim_customer.write.mode("overwrite").parquet("hdfs://localhost:9000/data/parquet/dim_customer")
dim_vehicle.write.mode("overwrite").parquet("hdfs://localhost:9000/data/parquet/dim_vehicle")
fact_policy.write.mode("overwrite").parquet("hdfs://localhost:9000/data/parquet/fact_policy")

print("--- Fact & Dimension Tables saved as Parquet on HDFS ---")

# 5. تسجيل الجداول كـ Temporary Views لإجراء استعلامات SQL
dim_customer.createOrReplaceTempView("dim_customer")
dim_vehicle.createOrReplaceTempView("dim_vehicle")
fact_policy.createOrReplaceTempView("fact_policy")

# 6. إجراء استعلام تحليلي (SQL Analytics) يربط الـ Fact بـ 2 Dimensions (JOIN)
analytics_df = spark.sql("""
    SELECT 
        c.Gender,
        v.Vehicle_Age,
        v.Vehicle_Damage,
        COUNT(f.customer_id) as total_policies,
        ROUND(AVG(f.Annual_Premium), 2) as avg_premium,
        ROUND(AVG(c.Age), 2) as avg_customer_age
    FROM fact_policy f
    JOIN dim_customer c ON f.customer_id = c.id
    JOIN dim_vehicle v ON f.vehicle_id = v.vehicle_id
    GROUP BY c.Gender, v.Vehicle_Age, v.Vehicle_Damage
    ORDER BY total_policies DESC
""")

print("=== Analytics Result (Star Schema JOIN Query) ===")
analytics_df.show()

# 7. حفظ الناتج التحليلي على HDFS كـ CSV
output_csv_path = "hdfs://localhost:9000/data/output/star_schema_analytics_csv"
analytics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_csv_path)

print("--- Analytics Output Saved to HDFS as CSV ---")

spark.stop()
