import sys
from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder.getOrCreate()


def read_json(spark, json_path: str):
    print(f" Reading JSON from : {json_path}")
    df = spark.read.option("multiLine", "true").json(json_path)
    print(f" JSON read success — Row count: {df.count()}")
    df.printSchema()
    return df

    
def write_delta(df, delta_path: str):
    print(f" Writing Delta to  : {delta_path}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(delta_path)
    print(f" Delta written successfully!")


def register_external_table(spark, catalog: str, schema: str, table: str, delta_path: str):
    full_table_name = f"{catalog}.{schema}.{table}"
    print(f" Registering UC External Table : {full_table_name}")

    # Create schema if not exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f" Schema ready : {catalog}.{schema}")

    # Drop existing table if exists (re-register)
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    print(f" Dropped existing table (if any)")

    # Register External Table
    spark.sql(f"""
        CREATE EXTERNAL TABLE {full_table_name}
        USING DELTA
        LOCATION '{delta_path}'
    """)
    print(f" External Table registered : {full_table_name}")

    # Verify
    print(f"\n Table Details:")
    spark.sql(f"DESCRIBE EXTENDED {full_table_name}").show(truncate=False)


def main():
    if len(sys.argv) != 6:
        print(" Usage: python json_to_external_table.py <json_path> <delta_path> <catalog> <schema> <table>")
        sys.exit(1)

    json_path    = sys.argv[1]
    delta_path   = sys.argv[2]
    catalog_name = sys.argv[3]
    schema_name  = sys.argv[4]
    table_name   = sys.argv[5]

    print("════════════════════════════════════════")
    print("  JSON → Delta → UC External Table")
    print("════════════════════════════════════════")
    print(f"  JSON Path  : {json_path}")
    print(f"  Delta Path : {delta_path}")
    print(f"  UC Table   : {catalog_name}.{schema_name}.{table_name}")
    print("════════════════════════════════════════\n")

    spark = get_spark()

    # Step 1: Read JSON
    df = read_json(spark, json_path)

    # Step 2: Write Delta
    write_delta(df, delta_path)

    # Step 3: Register External Table in UC
    register_external_table(spark, catalog_name, schema_name, table_name, delta_path)

    print("\n Done! External Table is ready in Unity Catalog.")

if __name__ == "__main__":
    main()
