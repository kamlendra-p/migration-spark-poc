#!/usr/bin/env python3
"""
Detailed investigation of MongoDB data structure and PostgreSQL mapping
This script helps understand the data transformation and identify potential issues
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, size, collect_list
import psycopg2
import json

# ---------------- CONFIG ----------------
MONGO_URI = "mongodb://localhost:27017/demo.ratePushResults2"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/rate-management"
PG_USER = "duetto"
PG_PASS = "password"
PG_TABLE = "rate_changes"

# Connector versions
MONGO_CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0"
POSTGRES_DRIVER = "org.postgresql:postgresql:42.6.0"

def analyze_mongodb_structure():
    """Analyze MongoDB document structure"""
    print("=" * 60)
    print("📊 MONGODB DATA STRUCTURE ANALYSIS")
    print("=" * 60)
    
    spark = (
        SparkSession.builder
        .appName("MongoDBInvestigation")
        .master("local[*]")
        .config("spark.jars.packages", f"{MONGO_CONNECTOR},{POSTGRES_DRIVER}")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .getOrCreate()
    )
    
    try:
        df = spark.read.format("mongodb").load()
        total_records = df.count()
        print(f"📈 Total records: {total_records:,}")
        
        print("\n🔍 Schema Analysis:")
        df.printSchema()
        
        print("\n📋 Sample Documents (first 3):")
        sample_docs = df.limit(3).collect()
        for i, row in enumerate(sample_docs, 1):
            print(f"\n--- Document {i} ---")
            print(json.dumps(row.asDict(), indent=2, default=str))
        
        # Analyze nested structure patterns
        print("\n🔎 Nested Structure Analysis:")
        
        # Check v.ir array length distribution
        ir_lengths = df.select(size(col("v.ir")).alias("ir_length")).groupBy("ir_length").count().orderBy("ir_length")
        print("\nv.ir array length distribution:")
        ir_lengths.show()
        
        # Check for sr vs er patterns
        has_sr = df.filter(col("v.ir")[0]["v"]["sr"].isNotNull()).count()
        has_er = df.filter(col("v.ir")[0]["v"]["er"].isNotNull()).count()
        print(f"\nRecords with v.ir[0].v.sr: {has_sr:,}")
        print(f"Records with v.ir[0].v.er: {has_er:,}")
        
        # Check rate codes structure
        print("\n💰 Rate Codes Analysis:")
        
        # Sample rate codes from sr path
        sr_rate_codes = df.filter(col("v.ir")[0]["v"]["sr"].isNotNull()) \
                         .select(expr("transform(flatten(transform(v.ir, x -> coalesce(x.v.sr.ir, array()))), y -> y.rc)").alias("rate_codes")) \
                         .filter(size(col("rate_codes")) > 0) \
                         .limit(5)
        
        print("Sample rate codes from SR path:")
        sr_rate_codes.show(truncate=False)
        
        # Check hotel ID patterns
        print("\n🏨 Hotel ID Analysis:")
        hotel_ids = df.select(col("v.h").alias("hotel_id")).distinct().limit(10)
        print("Sample hotel IDs:")
        hotel_ids.show()
        
        # Check currency patterns
        print("\n💱 Currency Analysis:")
        currencies = df.filter(col("v.ir")[0]["v"]["sr"]["ur"]["r"]["c"].isNotNull()) \
                      .select(col("v.ir")[0]["v"]["sr"]["ur"]["r"]["c"].alias("currency")) \
                      .groupBy("currency").count().orderBy(col("count").desc())
        print("Currency distribution:")
        currencies.show()
        
        # Check date ranges
        print("\n📅 Date Range Analysis:")
        date_stats = df.select(
            expr("min(to_date(_id.a))").alias("min_date"),
            expr("max(to_date(_id.a))").alias("max_date"),
            expr("count(distinct to_date(_id.a))").alias("unique_dates")
        )
        print("Date statistics:")
        date_stats.show()
        
    except Exception as e:
        print(f"❌ Error analyzing MongoDB: {e}")
    finally:
        spark.stop()

def analyze_postgresql_data():
    """Analyze PostgreSQL migrated data"""
    print("\n" + "=" * 60)
    print("🗄️  POSTGRESQL DATA ANALYSIS")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="rate-management",
            user=PG_USER,
            password=PG_PASS
        )
        cursor = conn.cursor()
        
        # Basic statistics
        cursor.execute(f"SELECT COUNT(*) FROM {PG_TABLE}")
        total_records = cursor.fetchone()[0]
        print(f"📈 Total migrated records: {total_records:,}")
        
        # Table structure
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = '{PG_TABLE}' 
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        print(f"\n🏗️  Table structure ({PG_TABLE}):")
        for col_name, data_type, nullable, default in columns:
            print(f"  {col_name:<25} {data_type:<20} NULL: {nullable:<3} Default: {default}")
        
        # Data quality checks
        print("\n🔍 Data Quality Analysis:")
        
        # Check for null values in key fields
        key_fields = ['hotel_id', 'stay_date', 'rate_push_transaction_id', 'new_published_rate']
        for field in key_fields:
            cursor.execute(f"SELECT COUNT(*) FROM {PG_TABLE} WHERE {field} IS NULL")
            null_count = cursor.fetchone()[0]
            null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
            print(f"  {field:<25} NULL values: {null_count:,} ({null_percentage:.1f}%)")
        
        # Hotel distribution
        cursor.execute(f"""
            SELECT hotel_id, COUNT(*) as record_count 
            FROM {PG_TABLE} 
            GROUP BY hotel_id 
            ORDER BY record_count DESC 
            LIMIT 10
        """)
        hotel_dist = cursor.fetchall()
        
        print(f"\n🏨 Top 10 Hotels by Record Count:")
        for hotel_id, count in hotel_dist:
            print(f"  Hotel {hotel_id}: {count:,} records")
        
        # Date range analysis
        cursor.execute(f"""
            SELECT 
                MIN(stay_date) as min_date,
                MAX(stay_date) as max_date,
                COUNT(DISTINCT stay_date) as unique_dates
            FROM {PG_TABLE}
        """)
        date_stats = cursor.fetchone()
        print(f"\n📅 Date Statistics:")
        print(f"  Date range: {date_stats[0]} to {date_stats[1]}")
        print(f"  Unique dates: {date_stats[2]:,}")
        
        # Rate analysis
        cursor.execute(f"""
            SELECT 
                AVG(new_published_rate) as avg_rate,
                MIN(new_published_rate) as min_rate,
                MAX(new_published_rate) as max_rate,
                COUNT(*) FILTER (WHERE new_published_rate > 0) as positive_rates
            FROM {PG_TABLE}
            WHERE new_published_rate IS NOT NULL
        """)
        rate_stats = cursor.fetchone()
        
        print(f"\n💰 Rate Statistics:")
        if rate_stats[0]:
            print(f"  Average rate: ${rate_stats[0]:.2f}")
            print(f"  Rate range: ${rate_stats[1]:.2f} - ${rate_stats[2]:.2f}")
            print(f"  Positive rates: {rate_stats[3]:,}")
        
        # Currency analysis
        cursor.execute(f"""
            SELECT hotel_currency_code, COUNT(*) as count
            FROM {PG_TABLE}
            WHERE hotel_currency_code IS NOT NULL
            GROUP BY hotel_currency_code
            ORDER BY count DESC
        """)
        currency_dist = cursor.fetchall()
        
        print(f"\n💱 Currency Distribution:")
        for currency, count in currency_dist:
            print(f"  {currency}: {count:,} records")
        
        # Rate codes analysis
        cursor.execute(f"""
            SELECT 
                COUNT(*) FILTER (WHERE rate_codes IS NOT NULL AND array_length(rate_codes, 1) > 0) as with_rate_codes,
                COUNT(*) FILTER (WHERE rate_codes IS NULL OR array_length(rate_codes, 1) IS NULL) as without_rate_codes
            FROM {PG_TABLE}
        """)
        rate_codes_stats = cursor.fetchone()
        
        print(f"\n🏷️  Rate Codes Statistics:")
        print(f"  Records with rate codes: {rate_codes_stats[0]:,}")
        print(f"  Records without rate codes: {rate_codes_stats[1]:,}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error analyzing PostgreSQL: {e}")

def compare_data_consistency():
    """Compare MongoDB and PostgreSQL data for consistency"""
    print("\n" + "=" * 60)
    print("🔄 DATA CONSISTENCY COMPARISON")
    print("=" * 60)
    
    # This would involve complex comparisons - placeholder for now
    print("🔍 Consistency checks to implement:")
    print("  ✓ Record count matching")
    print("  ⏳ Hotel ID preservation")
    print("  ⏳ Date range consistency") 
    print("  ⏳ Rate value accuracy")
    print("  ⏳ Transaction ID mapping")
    print("\n💡 Run verify_migration.py for basic count comparison")

def main():
    print("🔬 DETAILED MIGRATION INVESTIGATION")
    print("=" * 80)
    print("This script provides deep analysis of your MongoDB to PostgreSQL migration")
    print()
    
    choice = input("Select analysis type:\n1. MongoDB structure\n2. PostgreSQL data\n3. Both\nChoice (1-3): ")
    
    if choice in ['1', '3']:
        analyze_mongodb_structure()
    
    if choice in ['2', '3']:
        analyze_postgresql_data()
    
    if choice == '3':
        compare_data_consistency()
    
    print("\n✅ Investigation complete!")
    print("💡 Use this information to optimize your migration process")

if __name__ == "__main__":
    main()