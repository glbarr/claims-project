from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("ClaimsProcessor") \
        .getOrCreate()

    claims = spark.read.csv("data/input/claims_data.csv", header=True, inferSchema=True)
    claims.show()
    claims.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()