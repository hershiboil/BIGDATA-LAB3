from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# Initialize the Spark Session
spark = SparkSession.builder.appName("Flood Control").getOrCreate()

# Read the file
df = spark.read.csv(
    "C:\\Users\\Hershey\\big data act 3\\BIGDATA-LAB3\\dpwh_flood_control_projects copy.csv", 
    header=True, 
    inferSchema=True,
    quote='"', 
    escape='"', 
    multiLine=True
)

# --- SAFE CLEANING START ---
# Use expr("try_cast(...)") so that malformed text becomes NULL instead of crashing
df_cleaned = df.withColumn("ApprovedBudgetForContract", expr("try_cast(ApprovedBudgetForContract as double)"))

# Remove the NULLs (the rows that were malformed text)
df_final = df_cleaned.filter(col("ApprovedBudgetForContract").isNotNull())
# --- SAFE CLEANING END ---

# Range Partitioning
df_range = df_final.repartitionByRange(3, "ApprovedBudgetForContract")

df_with_partitions = df_range.withColumn("partition_num", spark_partition_id())
df_with_partitions.groupBy("partition_num").count().orderBy("partition_num").show()

#-------------------------------------------------------------PARTITIONING BY KEY (REGION)-----------------------------------------------------------------
# 1. Calculate how many drawers we need (one for each region)
num_regions = df_final.select("Region").distinct().count()

# 2. Partition the data so EVERY region gets its own drawer
df_list = df_final.repartition(num_regions, "Region")

# 3. Display the result
# 1. Perform the aggregation
df_insights = df_list.groupBy("Region").agg(
    count("Region").alias("Total_Projects"),
    avg("ApprovedBudgetForContract").alias("Avg_Raw"),
    sum("ApprovedBudgetForContract").alias("Total_Raw")
)

# 2. Sort by the numeric column FIRST
df_sorted_numeric = df_insights.orderBy(col("Total_Raw").desc())


# 2. Format the numbers into Peso strings
# format_number(column, 2) adds commas and rounds to 2 decimal places
df_formatted = df_sorted_numeric.withColumn(
    "Average_Budget", 
    concat(lit("₱ "), format_number(col("Avg_Raw"), 2))
).withColumn(
    "Total_Regional_Budget", 
    concat(lit("₱ "), format_number(col("Total_Raw"), 2))
)


df_final_display = df_formatted.select(
    "Region", 
    "Total_Projects", 
    "Average_Budget", 
    "Total_Regional_Budget"
)

# 3. ACTION: Show the clean table
print("Top 5 Regions by Total Budget:")
df_final_display.show(5, truncate=False)