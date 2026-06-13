"""
SparkSession factory.

Why a factory function instead of creating SparkSession everywhere?
Every Spark job needs a session. If each job creates its own with different
settings, you get inconsistent behavior. One factory, one configuration,
used by every job.

SparkSession is a singleton inside the JVM — calling builder.getOrCreate()
a second time returns the existing session, it never creates two.
"""

from pyspark.sql import SparkSession
import os


def get_spark_session(app_name: str = "AIEcosystemPlatform", local: bool = True) -> SparkSession:
    """
    Creates or returns an existing SparkSession.

    Parameters
    ----------
    app_name : str
        Name shown in the Spark UI — useful for identifying your job
        among many running jobs in a shared cluster.
    local : bool
        True  → runs entirely on your laptop (local[*] uses all CPU cores)
        False → connects to the Docker Spark cluster (spark://spark-master:7077)

    local[*] explained:
        The asterisk means "use as many threads as CPU cores available".
        local[2] would use exactly 2 threads.
        In production this would be spark://spark-master:7077.
    """
    master = "local[*]" if local else os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # These configs control memory for the driver process
        # (the Python process that coordinates the job)
        .config("spark.driver.memory", "2g")
        # Suppress verbose INFO logs — you still see errors and your print() output
        .config("spark.sql.repl.eagerEval.enabled", "true")
        # Parquet is the best format for analytics — columnar, compressed
        # Setting this means spark.write defaults to parquet
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Avoid creating too many small output files
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    # Set log level to WARN so we don't drown in INFO messages
    # You'll still see errors and your own print()/show() output clearly
    spark.sparkContext.setLogLevel("WARN")

    return spark
