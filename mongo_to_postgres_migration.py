from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, when, udf, to_date, flatten, coalesce
)
from pyspark.sql.types import StringType, DoubleType, ArrayType

# ---------------- CONFIG ----------------
MONGO_URI = "mongodb://localhost:27017/demo.ratePushResults2"   # change db name
POSTGRES_URL = "jdbc:postgresql://localhost:5432/rate-management"     # change db name
PG_USER = "duetto"
PG_PASS = "password"
PG_TABLE = "rate_changes"

# Connector versions may vary by Spark build
MONGO_CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0"
POSTGRES_DRIVER = "org.postgresql:postgresql:42.6.0"

# ---------------- SPARK INIT ----------------
spark = (
    SparkSession.builder
    .appName("MongoToPostgresMigration")
    .master("local[*]")
    .config("spark.jars.packages", f"{MONGO_CONNECTOR},{POSTGRES_DRIVER}")
    .config("spark.mongodb.read.connection.uri", MONGO_URI)
    .getOrCreate()
)

# ---------------- READ FROM MONGO ----------------
df = spark.read.format("mongodb").load()

# Optional: inspect schema
# df.printSchema()

# ---------------- UDFs ----------------
def to_pg_array_literal(arr):
    if arr is None:
        return None
    return "{" + ",".join([str(x) for x in arr]) + "}"

to_pg_array_literal_udf = udf(to_pg_array_literal, StringType())

# ---------------- TRANSFORMATION ----------------
# Flatten rate_codes: v.ir[*].v.sr.ir[*].rc (handle both sr and er)
rate_codes_df = df.withColumn(
    "rate_codes_sr",
    expr("transform(flatten(transform(v.ir, x -> coalesce(x.v.sr.ir, array()))), y -> y.rc)")
).withColumn(
    "rate_codes_er", 
    expr("transform(flatten(transform(v.ir, x -> coalesce(x.v.er.ir, array()))), y -> y.rc)")
).withColumn(
    "rate_codes",
    expr("concat(coalesce(rate_codes_sr, array()), coalesce(rate_codes_er, array()))")
).drop("rate_codes_sr", "rate_codes_er")

# Core mapping
mapped = rate_codes_df.select(
    # hotel_id: remove "ho" prefix if present
    expr("regexp_replace(v.h, '^ho', '')").alias("hotel_id"),

    # stay_date: extract date part from _id.a
    to_date(col("_id.a")).alias("stay_date"),

    # rate_push_transaction_id (try sr first, then er)
    coalesce(
        col("v.ir")[0]["v"]["sr"]["tid"],
        col("v.ir")[0]["v"]["er"]["tid"]
    ).alias("rate_push_transaction_id"),

    # modified_timestamp
    when(col("v.a").isNotNull(), col("v.a")).otherwise(col("_id.a")).alias("modified_timestamp"),

    # manually_published_by
    lit(None).cast(StringType()).alias("manually_published_by"),

    # rate_publish_reason
    lit(None).cast(StringType()).alias("rate_publish_reason"),

    # target_id (try sr first, then er)
    coalesce(
        col("v.ir")[0]["v"]["sr"]["ri"],
        col("v.ir")[0]["v"]["er"]["ri"]
    ).alias("target_id"),

    # rate_codes as Postgres array 
    col("rate_codes").alias("rate_codes"),

    # room_type_id (try sr first, then er)
    coalesce(
        col("v.ir")[0]["v"]["sr"]["pid"],
        col("v.ir")[0]["v"]["er"]["pid"]
    ).alias("room_type_id"),

    # prev_published_rate (provide default value for NOT NULL constraint)
    lit(0.0).cast(DoubleType()).alias("prev_published_rate"),

    # prev_published_rate_sent (provide default value for NOT NULL constraint)
    lit(0.0).cast(DoubleType()).alias("prev_published_rate_sent"),

    # new_published_rate (try sr first, then er)
    coalesce(
        col("v.ir")[0]["v"]["sr"]["ur"]["r"]["v"],
        col("v.ir")[0]["v"]["er"]["ur"]["r"]["v"]
    ).cast(DoubleType()).alias("new_published_rate"),

    # new_published_rate_sent (provide default value for NOT NULL constraint)
    lit(0.0).cast(DoubleType()).alias("new_published_rate_sent"),

    # hotel_currency_code (try sr first, then er)
    coalesce(
        col("v.ir")[0]["v"]["sr"]["ur"]["r"]["c"],
        col("v.ir")[0]["v"]["er"]["ur"]["r"]["c"]
    ).alias("hotel_currency_code"),

    # rate_sent_currency_code (try sr first, then er)
    coalesce(
        col("v.ir")[0]["v"]["sr"]["ur"]["r"]["c"],
        col("v.ir")[0]["v"]["er"]["ur"]["r"]["c"]
    ).alias("rate_sent_currency_code"),

    # optimization_time
    lit(None).cast(StringType()).alias("optimization_time"),

    # optimization_type
    lit(None).cast(StringType()).alias("optimization_type")
)

# ---------------- WRITE TO POSTGRES ----------------
jdbc_props = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}

mapped.write.jdbc(
    url=POSTGRES_URL,
    table=PG_TABLE,
    mode="append",
    properties=jdbc_props
)

spark.stop()
