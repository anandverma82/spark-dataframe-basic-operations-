# Databricks notebook source
df=spark.read\
    .option("header" , "true")\
        .option("inferschema" , "true")\
            .csv("/Volumes/project_cl/product_schema/product-volume/sales_data2.csv")
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ### string functions 
# MAGIC
# MAGIC String functions are used to manipulate, clean, format, and transform text (string) data in PySpark DataFrames.
# MAGIC
# MAGIC they are heavily used by data engineers 
# MAGIC
# MAGIC Data cleaning
# MAGIC
# MAGIC Data standardization
# MAGIC
# MAGIC ETL pipelines
# MAGIC
# MAGIC Preparing data for analytics and reporting

# COMMAND ----------

from pyspark.sql.functions import *

# upper( ) -> converts a string uppercase 
df.select( "*",upper(col("customer_name").alias("new_name"))).show(2)

#lower() -> converts a string to lowercase
df.select(lower(col("country").alias("new_name"))).show(2)

#concat()  -> concatenates(joins) multiple columns without any separator 
# if any column is null, the result becomes null
df.select(concat(col("country"),col("customer_name").alias("full_name"))).show(2)


# concat_ws() -> concatenates columns using a separtor 
# null values are ignored , safer than concat()
df.select(concat_ws("-", col("country"),col("customer_name").alias("new_name"))).show(2)


#substring()  -> extracts a part of a string
# substring(column,start_posotion,length)
# indexing start from 1,not0
df.select(substring(col("customer_name"),1,4).alias("new_name")).show(2)


# trim()->removes spaces from both left and right sides 
df.select(trim(col("customer_name").alias("new_name"))).show(2)

 
 # ltrim() -> removes spaces from the left side only
df.select(ltrim(col("customer_name").alias("new_name"))).show(2)

# rtrim() -> removes spaces fromthe right side only 
df.select(rtrim(col("customer_name").alias("new_name"))).show(2)


#length() -> returns the number of charactrs in a string
# spaces are also counted
df.select(length(col("customer_name").alias("name_length"))).show(2)


df.select (
    upper(col("customer_name").alias("new_name")),
    concat_ws("-",col("customer_name"),col("state").alias("new_name")),
    length(col("customer_name").alias("count"))
).show(2)


# COMMAND ----------

# MAGIC %md
# MAGIC # what is a numeric functions 
# MAGIC
# MAGIC  numeric functions are used to perform mathematical operations on numeric columns such as integers , floats , and doubles 

# COMMAND ----------

# abs()-> used to clean negative values (balances , adjustments , corrections)
# -2000=2000
df.select(abs("price").alias("new_salary")).show(2)


# round() -> rounds a number to given decimal places 
#3455.3445=3455.34
df.select(round(col("price").alias("new_paice"))).show(2)


#ceil() -> rounds up to the nearest integer
#45.1=46
df.select(ceil(col("price").alias("new_price"))).show(2)

#floor()-> rounds down to the nearest integer
#45.9=45
df.select(floor(col("price").alias("new_price"))).show(2)


# sqrt()-> returns the square root
# 100->10
df.select(sqrt(col("price"))).show(2)


# power()/pow()  -> raises a number to a power
# 10=100
df.select(power(col("price"),2)).show(2)





# COMMAND ----------

# MAGIC %md
# MAGIC # what is time & date functions
# MAGIC
# MAGIC date & time functions are used to create , extract , convert , compare , and manipulate date and timestamp values in pyspark dataframes.

# COMMAND ----------

# ===============================
# IMPORT REQUIRED FUNCTIONS
# ===============================
from pyspark.sql.functions import *


# ===============================
# STEP 1: CREATE DATAFRAME
# ===============================
data = [
    (1, "2026-01-20", "2026-01-20 10:30:45"),
    (2, "2025-12-15", "2025-12-15 18:15:10"),
    (3, "2024-07-01", "2024-07-01 09:05:00")
]

columns = ["id", "order_date", "event_time"]

df = spark.createDataFrame(data, columns)
df.show()



# ===============================
# STEP 2: CONVERT STRING → DATE & TIMESTAMP
# to_date() -> string ko date format m convert karta hai
# to_timestamp() -> string ko timestamp m convert karta hai 
# ===============================
df_dt = df.withColumn(
    "order_dt", to_date(col("order_date"))        # string → date
).withColumn(
    "event_ts", to_timestamp(col("event_time"))   # string → timestamp
)

df_dt.show(truncate=False)


# ===============================
# STEP 3: CURRENT DATE & TIMESTAMP
# current_date() -> aaj ki date return karta hai
# current_timestamp()-> current date + time(timestamp) return karta h
# ===============================
df_dt.select(
    current_date().alias("today_date"),
    current_timestamp().alias("current_time")
).show(truncate=False)



# ===============================
# STEP 4: EXTRACT DATE PARTS
# year(), month() , dayofmonth date se nikalta hai 
# ===============================
df_dt.select(
    col("id"),
    col("order_dt"),
    year(col("order_dt")).alias("year"),
    month(col("order_dt")).alias("month"),
    dayofmonth(col("order_dt")).alias("day"),
    dayofweek(col("order_dt")).alias("day_of_week"),
    dayofyear(col("order_dt")).alias("day_of_year"),
    weekofyear(col("order_dt")).alias("week_of_year")
).show()



# ===============================
# STEP 5: DATE DIFFERENCE
# datediff()-> do dates ke beech differance(days)
# ===============================
df_dt.select(
    col("id"),
    col("order_dt"),
    datediff(current_date(), col("order_dt")).alias("days_difference")
).show()


# ===============================
# STEP 6: ADD / SUBTRACT DAYS
# date_add()-> date m days add karta hai 
# date_sub()-> date m days minus karta hai
# ===============================
df_dt.select(
    col("order_dt"),
    date_add(col("order_dt"), 7).alias("plus_7_days"),
    date_sub(col("order_dt"), 7).alias("minus_7_days")
).show()



# ===============================
# STEP 7: ADD MONTHS
# add_months( ) date m months add/ subtract karta hai 
# ===============================
df_dt.select(
    col("order_dt"),
    add_months(col("order_dt"), 2).alias("plus_2_months"),
    add_months(col("order_dt"), -2).alias("minus_2_months")
).show()



# ===============================
# STEP 8: LAST DAY OF MONTH
# last_day( ) month ka last date nikalta ahi
# ===============================
df_dt.select(
    col("order_dt"),
    last_day(col("order_dt")).alias("month_end_date")
).show()


# ===============================
# STEP 9: NEXT SPECIFIC DAY
# ===============================
df_dt.select(
    col("order_dt"),
    next_day(col("order_dt"), "Sunday").alias("next_sunday")
).show()



# COMMAND ----------

