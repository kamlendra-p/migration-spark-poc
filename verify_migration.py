#!/usr/bin/env python3
"""
Verification script to check the actual data transfer from MongoDB to PostgreSQL
"""
from pyspark.sql import SparkSession
import psycopg2

# ---------------- CONFIG ----------------
MONGO_URI = "mongodb://localhost:27017/demo.ratePushResults2"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/rate-management"
PG_USER = "duetto"
PG_PASS = "password"
PG_TABLE = "rate_changes"

# For direct PostgreSQL connection (not through Spark)
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "rate-management"

# Connector versions
MONGO_CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0"
POSTGRES_DRIVER = "org.postgresql:postgresql:42.6.0"

print("=== Data Transfer Verification ===\n")

# ---------------- CHECK MONGODB COUNT ----------------
print("1. Checking MongoDB source data...")
spark = (
    SparkSession.builder
    .appName("DataVerification")
    .master("local[*]")
    .config("spark.jars.packages", f"{MONGO_CONNECTOR},{POSTGRES_DRIVER}")
    .config("spark.mongodb.read.connection.uri", MONGO_URI)
    .getOrCreate()
)

try:
    df = spark.read.format("mongodb").load()
    mongo_count = df.count()
    print(f"✅ MongoDB records: {mongo_count}")
except Exception as e:
    print(f"❌ MongoDB connection failed: {e}")
    mongo_count = 0

spark.stop()

# ---------------- CHECK POSTGRESQL COUNT ----------------
print("\n2. Checking PostgreSQL destination data...")
try:
    # Direct connection to PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    cursor = conn.cursor()
    
    # Count records in the target table
    cursor.execute(f"SELECT COUNT(*) FROM {PG_TABLE}")
    pg_count = cursor.fetchone()[0]
    print(f"✅ PostgreSQL records: {pg_count}")
    
    # Get sample data to verify structure
    cursor.execute(f"SELECT * FROM {PG_TABLE} LIMIT 3")
    sample_records = cursor.fetchall()
    
    # Get column names
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{PG_TABLE}' ORDER BY ordinal_position")
    column_names = [row[0] for row in cursor.fetchall()]
    
    print(f"\n3. Sample PostgreSQL data:")
    print(f"Columns: {column_names}")
    for i, record in enumerate(sample_records, 1):
        print(f"Record {i}: {record}")
    
    # Check for any errors in the table
    cursor.execute(f"SELECT hotel_id, stay_date, rate_push_transaction_id FROM {PG_TABLE} ORDER BY hotel_id LIMIT 5")
    sample_ids = cursor.fetchall()
    print(f"\n4. Sample hotel IDs and dates:")
    for record in sample_ids:
        print(f"Hotel: {record[0]}, Date: {record[1]}, Transaction: {record[2]}")
        
    conn.close()
    
except Exception as e:
    print(f"❌ PostgreSQL connection failed: {e}")
    pg_count = 0

# ---------------- SUMMARY ----------------
print(f"\n=== Transfer Summary ===")
print(f"MongoDB source:      {mongo_count:,} records")
print(f"PostgreSQL target:   {pg_count:,} records")

if mongo_count > 0:
    transfer_percentage = (pg_count / mongo_count) * 100
    print(f"Transfer success:    {transfer_percentage:.1f}%")
    
    if pg_count < mongo_count:
        missing_count = mongo_count - pg_count
        print(f"❌ Missing records:  {missing_count:,}")
        print(f"\nPossible causes:")
        print("1. Partial write due to errors during transfer")
        print("2. Transaction rollback due to constraint violations")
        print("3. Data filtering during transformation")
        print("4. Connection timeout during large data transfer")
        print("5. Duplicate key violations (if running multiple times)")
    else:
        print("✅ Transfer appears complete!")

# Check if there were any recent errors or if process was interrupted
print(f"\n=== Recommendations ===")
if pg_count < mongo_count:
    print("1. Check Spark logs for any errors or warnings")
    print("2. Run migration again with smaller batch sizes")
    print("3. Check PostgreSQL logs for constraint violations")
    print("4. Consider using 'overwrite' mode instead of 'append'")
    print("5. Add progress monitoring during the transfer")