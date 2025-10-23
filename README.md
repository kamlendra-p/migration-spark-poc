# MongoDB to PostgreSQL Migration with Apache Spark

This project provides a scalable solution for migrating data from MongoDB to PostgreSQL using Apache Spark and PySpark. The script handles complex nested MongoDB documents and transforms them into a structured PostgreSQL schema.

## 📋 Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Mapping](#data-mapping)
- [Troubleshooting](#troubleshooting)
- [Production Considerations](#production-considerations)
- [Contributing](#contributing)

## 🔧 Prerequisites

### System Requirements
- **Python**: 3.8 or higher
- **Java**: 8, 11, or 17 (required for Spark)
- **MongoDB**: Running instance with accessible data
- **PostgreSQL**: Running instance with target database created

### Check Your Setup
```bash
# Check Python version
python3 --version

# Check Java version
java -version

# Check MongoDB connection
mongosh --eval "db.adminCommand('ismaster')"

# Check PostgreSQL connection
psql --version
```

## 🚀 Installation

### 1. Clone or Download the Project
```bash
git clone <your-repo-url>
cd spark-poc
```

### 2. Create Virtual Environment
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate     # On Windows
```

### 3. Install Dependencies
```bash
# Install PySpark and dependencies
pip install pyspark==4.0.1 py4j==0.10.9.9 psycopg2-binary

# Verify installation
python3 -c "from pyspark.sql import SparkSession; print('PySpark installed successfully!')"
```

### 4. Verify Spark Connectors
The script automatically downloads required connectors:
- **MongoDB Connector**: `org.mongodb.spark:mongo-spark-connector_2.13:10.4.0`
- **PostgreSQL Driver**: `org.postgresql:postgresql:42.6.0`

## ⚙️ Configuration

### 1. Update Connection Settings

Edit the configuration section in `mongo_to_postgres_migration.py`:

```python
# ---------------- CONFIG ----------------
MONGO_URI = "mongodb://localhost:27017/demo.ratePushResults2"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/rate-management"
PG_USER = "duetto"
PG_PASS = "password"
PG_TABLE = "rate_changes"
```

### 2. MongoDB Configuration
Ensure your MongoDB URI follows this format:
```
mongodb://[username:password@]host[:port]/database.collection
```

**Examples:**
```python
# Local MongoDB without authentication (CURRENT CONFIG)
MONGO_URI = "mongodb://localhost:27017/demo.ratePushResults2"

# MongoDB with authentication
MONGO_URI = "mongodb://user:password@localhost:27017/demo.ratePushResults2"

# MongoDB Atlas (cloud)
MONGO_URI = "mongodb+srv://user:password@cluster.mongodb.net/demo.ratePushResults2"
```

### 3. PostgreSQL Configuration
Ensure your PostgreSQL database and target table exist:

```sql
-- Create database (if not exists)
CREATE DATABASE "rate-management";

-- Create target table (example schema)
CREATE TABLE rate_changes (
    id SERIAL PRIMARY KEY,
    hotel_id VARCHAR(50),
    stay_date DATE,
    rate_push_transaction_id VARCHAR(100),
    modified_timestamp TIMESTAMP,
    manually_published_by VARCHAR(100),
    rate_publish_reason VARCHAR(255),
    target_id VARCHAR(100),
    rate_codes VARCHAR(255)[],  -- Array type
    room_type_id VARCHAR(50),
    prev_published_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    prev_published_rate_sent DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    new_published_rate DOUBLE PRECISION,
    new_published_rate_sent DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    hotel_currency_code VARCHAR(10),
    rate_sent_currency_code VARCHAR(10),
    optimization_time VARCHAR(100),
    optimization_type VARCHAR(100)
);
```

## 🎯 Usage

### 1. Basic Migration
```bash
# Activate virtual environment
source venv/bin/activate

# Run the migration
python3 mongo_to_postgres_migration.py
```

### 2. Verify Migration
Create a verification script to check the migration:

```python
# verify_migration.py
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("VerifyMigration").getOrCreate()

# Check MongoDB count
mongo_df = spark.read.format("mongodb").load()
mongo_count = mongo_df.count()
print(f"MongoDB records: {mongo_count:,}")

# Check PostgreSQL count
postgres_df = spark.read.jdbc(
    url="jdbc:postgresql://localhost:5432/rate-management",
    table="rate_changes",
    properties={"user": "duetto", "password": "password"}
)
postgres_count = postgres_df.count()
print(f"PostgreSQL records: {postgres_count:,}")

spark.stop()
```

### 3. Monitor Progress
For large datasets, monitor the Spark UI:
- Open browser to: `http://localhost:4040`
- Check job progress, stages, and executors

## 📊 Data Mapping

The migration script maps MongoDB document structure to PostgreSQL columns:

| MongoDB Path | PostgreSQL Column | Transformation |
|--------------|-------------------|----------------|
| `v.h` | `hotel_id` | Remove "ho" prefix if present |
| `_id.a` | `stay_date` | Convert to DATE |
| `v.ir[0].v.sr.tid` or `v.ir[0].v.er.tid` | `rate_push_transaction_id` | Coalesce sr/er paths |
| `v.a` or `_id.a` | `modified_timestamp` | Use v.a if available, else _id.a |
| `v.ir[*].v.sr.ir[*].rc` + `v.ir[*].v.er.ir[*].rc` | `rate_codes` | Flatten and combine arrays |
| `v.ir[0].v.sr.pid` or `v.ir[0].v.er.pid` | `room_type_id` | Coalesce sr/er paths |
| `v.ir[0].v.sr.ur.r.v` or `v.ir[0].v.er.ur.r.v` | `new_published_rate` | Cast to DOUBLE |
| `v.ir[0].v.sr.ur.r.c` or `v.ir[0].v.er.ur.r.c` | `hotel_currency_code` | Coalesce sr/er paths |

### Data Structure Example
```json
// MongoDB Document Structure
{
  "_id": {"a": "2024-01-15T10:30:00Z"},
  "v": {
    "h": "ho295764",
    "a": "2024-01-15T10:35:00Z",
    "ir": [{
      "v": {
        "sr": {
          "tid": "507f1f77bcf86cd799439011",
          "ri": "booking.com",
          "pid": "DELUXE",
          "ur": {"r": {"v": 150.00, "c": "USD"}},
          "ir": [{"rc": "BAR"}, {"rc": "FLEX"}]
        }
      }
    }]
  }
}
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Java Version Conflicts
```bash
# Check Java version
java -version

# If multiple Java versions, set JAVA_HOME
export JAVA_HOME=/path/to/java-11
```

#### 2. MongoDB Connection Issues
```bash
# Test MongoDB connection
mongosh "mongodb://localhost:27017/demo" --eval "db.ratePushResults2.count()"

# Check if collection exists
mongosh "mongodb://localhost:27017/demo" --eval "db.getCollectionNames()"
```

#### 3. PostgreSQL Connection Issues
```bash
# Test PostgreSQL connection
psql -h localhost -p 5432 -U duetto -d rate-management -c "SELECT version();"

# Check if table exists
psql -h localhost -p 5432 -U duetto -d rate-management -c "\dt"
```

#### 4. Memory Issues
If you encounter OutOfMemory errors:

```python
# Increase Spark memory settings
spark = (
    SparkSession.builder
    .appName("MongoToPostgresMigration")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.jars.packages", f"{MONGO_CONNECTOR},{POSTGRES_DRIVER}")
    .config("spark.mongodb.read.connection.uri", MONGO_URI)
    .getOrCreate()
)
```

#### 5. Data Type Errors
```sql
-- If PostgreSQL schema doesn't match, update table
ALTER TABLE rate_changes ALTER COLUMN rate_codes TYPE text[];
ALTER TABLE rate_changes ALTER COLUMN prev_published_rate SET DEFAULT 0.0;
```

### Debug Mode
Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add before transformations
logger.info(f"MongoDB records found: {df.count()}")
df.printSchema()
df.show(5, truncate=False)
```

## 🏭 Production Considerations

### For Large Datasets (1GB+)

#### 1. Use Cluster Mode
```python
spark = (
    SparkSession.builder
    .appName("MongoToPostgresMigration")
    .master("yarn")  # or "spark://cluster-url"
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "16g")
    .config("spark.executor.cores", "4")
    .config("spark.executor.instances", "10")
    # ... other configs
)
```

#### 2. Implement Batching
```python
# Process data in date-based batches
date_ranges = [
    ("2024-01-01", "2024-02-01"),
    ("2024-02-01", "2024-03-01"),
    # ... more ranges
]

for start_date, end_date in date_ranges:
    batch_df = df.filter(
        (col("_id.a") >= start_date) & (col("_id.a") < end_date)
    )
    # Process batch...
```

#### 3. Add Error Handling
```python
try:
    mapped.write.jdbc(
        url=POSTGRES_URL,
        table=PG_TABLE,
        mode="append",
        properties=jdbc_props
    )
    logger.info("Migration completed successfully")
except Exception as e:
    logger.error(f"Migration failed: {str(e)}")
    # Implement retry logic or rollback
```

#### 4. Optimize PostgreSQL
```sql
-- Disable indexes during bulk insert
DROP INDEX IF EXISTS idx_rate_changes_hotel_date;

-- Re-enable after migration
CREATE INDEX idx_rate_changes_hotel_date ON rate_changes(hotel_id, stay_date);

-- Consider table partitioning for very large datasets
```

### Performance Tips
- **Partition your data** by date or hotel_id
- **Increase batch size** for JDBC writes
- **Use appropriate number of executors** based on your cluster
- **Monitor Spark UI** for bottlenecks
- **Tune PostgreSQL** settings for bulk inserts

## 📝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Make your changes
4. Test thoroughly with sample data
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

If you encounter issues:

1. Check the [Troubleshooting](#troubleshooting) section
2. Verify your [Prerequisites](#prerequisites)
3. Test with a small data subset first
4. Check Spark logs in the console output
5. Open an issue with detailed error messages and configuration

---

**Note**: Always test migrations with a subset of data before running on production datasets. For datasets larger than 1TB, consider using managed ETL services like AWS Glue or Azure Data Factory.