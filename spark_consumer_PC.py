from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# 1. Ορισμός του Schema
schema = StructType() \
    .add("name", StringType()) \
    .add("dn", IntegerType()) \
    .add("orig", StringType()) \
    .add("dest", StringType()) \
    .add("t", DoubleType()) \
    .add("link", StringType()) \
    .add("x", DoubleType()) \
    .add("s", DoubleType()) \
    .add("v", DoubleType())

# 2. Δημιουργία Spark Session
spark = (
    SparkSession.builder
    .appName("UXSIM-Consumer")
    .master("spark://spark-master:7077")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .getOrCreate()
)

# 3. Σύνδεση στον Redpanda (Kafka-compatible)
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "uxsim")
    .option("startingOffsets", "latest")
    .load()
)

# ==============================================================================
# ΣΥΜΠΛΗΡΩΣΗ ΚΩΔΙΚΑ: 4. Parsing του JSON και Μετασχηματισμός
# Εδώ μετατρέπουμε τα δυαδικά δεδομένα (binary) του Kafka σε αναγνώσιμες στήλες
# ==============================================================================
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")
# ==============================================================================

# ==============================================================================
# ΣΥΜΠΛΗΡΩΣΗ ΚΩΔΙΚΑ: 5. Υπολογισμός Στατιστικών ανά Ακμή (link) και Χρόνο (t)
# Ομαδοποιούμε τα δεδομένα ανά χρόνο και δρόμο για να βρούμε πλήθος και μέση ταχύτητα
# ==============================================================================
stats = (
    parsed.groupBy("t", "link")
    .agg(
        count("*").alias("vcount"),
        avg("v").alias("vspeed")
    )
)
# ==============================================================================

# ==============================================================================
# ΣΥΜΠΛΗΡΩΣΗ ΚΩΔΙΚΑ: 6α. Αποθήκευση στη MongoDB των αρχικών δεδομένων (Raw)
# Αποθηκεύουμε κάθε εγγραφή όπως έρχεται στη συλλογή 'raw_data'
# ==============================================================================
query_raw = (
    parsed.writeStream
    .format("mongodb")
    .option("checkpointLocation", "/tmp/checkpoint_raw")
    .option("spark.mongodb.write.database", "traffic")
    .option("spark.mongodb.write.collection", "raw_data")
    .outputMode("append")
    .start()
)
# ==============================================================================

# ==============================================================================
# ΣΥΜΠΛΗΡΩΣΗ ΚΩΔΙΚΑ: 6β. Αποθήκευση στη MongoDB των επεξεργασμένων δεδομένων (Stats)
# Χρησιμοποιούμε 'update' mode γιατί τα στατιστικά αλλάζουν καθώς έρχονται νέα δεδομένα
# ==============================================================================
query_mongo = (
    stats.writeStream
    .format("mongodb")
    .option("checkpointLocation", "/tmp/checkpoint_stats")
    .option("spark.mongodb.write.database", "traffic")
    .option("spark.mongodb.write.collection", "stats")
    .outputMode("update")
    .start()
)
# ==============================================================================

# 7. Προβολή στην κονσόλα για debugging (προαιρετικά)
query_console = (
    stats.writeStream
    .format("console")
    .outputMode("update")
    .start()
)

# Αναμονή για τον τερματισμό όλων των queries
spark.streams.awaitAnyTermination()