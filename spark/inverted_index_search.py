import argparse
import logging
import os
import psutil
import requests
import sys
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, udf, explode, split, lower, regexp_replace, col, concat_ws, collect_list, concat, lit, sort_array
from pyspark.sql.types import StringType

#----------------------------#
#        CONFIGURATION       #
#----------------------------#
HDFS_BASE    = 'hdfs:///user/hadoop/'
DATA_DIR     = HDFS_BASE + 'inverted-index/data'
OUTPUT_BASE  = HDFS_BASE + 'inverted-index/output/'
LOG_DIR      = HDFS_BASE + 'inverted-index/log/'

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
printer = logging.getLogger(__name__)

#----------------------------#
#   HDFS UTILITY FUNCTIONS   #
#----------------------------#
def hdfs_dir_exists(sc, path):
    """Check if an HDFS path exists."""
    jvm = sc._jvm
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))

def list_hdfs_files(sc, path):
    """List all files (path, size) in HDFS directory."""
    jvm = sc._jvm
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    p = jvm.org.apache.hadoop.fs.Path(path)
    files = []
    if fs.exists(p):
        for status in fs.listStatus(p):
            if status.isFile():
                files.append((status.getPath().toString(), status.getLen()))
    return files

def choose_input_paths(sc, limit_mb=None):
    """Select input files up to limit_mb (if specified), else all."""
    if not hdfs_dir_exists(sc, DATA_DIR):
        raise HDFSPathNotFoundError(f"Data directory '{DATA_DIR}' does not exist.")
    if limit_mb is None:
        return [DATA_DIR + '/*']
    mb_bytes = limit_mb * 1024 * 1024
    files = sorted(list_hdfs_files(sc, DATA_DIR), key=lambda x: x[1])
    selected, total = [], 0
    for path, size in files:
        if total + size > mb_bytes:
            break
        selected.append(path)
        total += size
    return selected

def choose_output_path(sc):
    """Pick a new output directory of form output-sparkX where X increments."""
    idx = 0
    while hdfs_dir_exists(sc, OUTPUT_BASE + f'output-spark{idx}'):
        idx += 1
    return OUTPUT_BASE + f'output-spark{idx}', idx

def write_log_to_hdfs(sc, idx, execution_time, phys_mb, virt_mb):
    """
    Write a single-line CSV log into HDFS at log/log{idx}.csv.
    Overwrite if exists.
    """
    # Prepare CSV content
    header = "execution_time,physical_memory_mb,virtual_memory_mb\n"
    line = f"{execution_time:.3f},{phys_mb:.2f},{virt_mb:.2f}\n"
    content = header + line

    # Access HDFS via Hadoop API
    jvm = sc._jvm
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    log_path = LOG_DIR + f'log-spark{idx}.csv'
    hdfs_path = jvm.org.apache.hadoop.fs.Path(log_path)

    # Ensure log directory exists
    log_dir_path = jvm.org.apache.hadoop.fs.Path(LOG_DIR)
    if not fs.exists(log_dir_path):
        fs.mkdirs(log_dir_path)

    # Create or overwrite the log file
    out = fs.create(hdfs_path, True)
    out.write(bytearray(content, 'utf-8'))
    out.close()
    return log_path

#----------------------------#
#         EXCEPTION          #
#----------------------------#
class HDFSPathNotFoundError(Exception):
    pass

#----------------------------#
#         SPARK JOB          #
#----------------------------#
class InvertedIndexSearch:
    # Fields
    spark: SparkSession
    sc: SparkConf

    # Methods
    def __init__(self, app_name="InvertedIndexSearch", num_partitions = None):
        """Configure Spark and initialize InvetedIndexSearch class"""
        conf = SparkConf() \
            .setAppName(app_name) \
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .set("spark.kryoserializer.buffer.max", "512m")
        if num_partitions:
            conf = conf.set("spark.sql.shuffle.partitions", str(num_partitions))
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext

    def safe_read_text(self, file_path):
        """Read a text file; on error, skip and log a warning."""
        try:
            return self.spark.read.text(file_path)
        except Exception as e:
            printer.warning(f"Skipping file {file_path} due to read error: {e}")
            return None

    def safe_read_all(self, input_paths):
        """Read all valid text files and attach a 'filename' column."""
        dfs = []
        for path in input_paths:
            df = self.safe_read_text(path)
            if df is not None:
                dfs.append(
                    df.withColumn("filename", udf(lambda path: os.path.basename(path), StringType())(input_file_name()))
                )
        if not dfs:
            raise RuntimeError("No valid input files could be read")
        # Union all DataFrames
        df_all = dfs[0]
        for df in dfs[1:]:
            df_all = df_all.union(df)
        return df_all

    def build_index(self, input_paths, output_path, output_format='text'):
        """Build the inverse index"""
        """PHASE 1: MAP-LIKE PHASE - Preprocessing and Tokenization"""
        # Read files with filename column
        df = self.safe_read_all(input_paths)

        # Replace everything that is not letter/number with spaces and lowercase all
        # Split lines into words and explode into rows
        tokens = df.withColumn('word', explode(split(lower(regexp_replace(col('value'), r'[^A-Za-z0-9]', ' ')), '\\s+'))).filter(col('word') != '')

        """PHASE 2: REDUCE-LIKE PHASE - Building Posting Lists"""        
        # Count occurrences of each word per filename
        counts = tokens.groupBy('word', 'filename').count()

        # Create postings list and format output
        postings = (
            counts.select(
                col('word'),
                concat(col('filename'), lit(':'), col('count')).alias('posting')
            )
            .groupBy('word')
            .agg(sort_array(collect_list(col('posting'))).alias('postings_list'))
        )

        """PHASE 3: FORMAT OUTPUT"""
        # Write output in the specified format
        if output_format == 'text':
            # Format to final output lines
            formatted = postings.select(concat_ws('\t', col('word'), concat_ws('\t', col('postings_list'))))
            # Default format: plain text output
            formatted.write.text(output_path)
        elif output_format == 'json':
            # JSON output: word and list of postings as docs
            json_ready = postings.selectExpr("word", "postings_list as docs")
            json_ready.write.json(output_path)
        elif output_format == 'parquet':
            # Parquet output
            parquet_ready = postings.selectExpr("word", "postings_list as docs")
            parquet_ready.write.parquet(output_path)
        else:
            # Format to final output lines
            formatted = postings.select(concat_ws('\t', col('word'), concat_ws('\t', col('postings_list'))))
            formatted.write.text(output_path)

    def get_executors_memory_metrics(self):
        """
        Query Spark UI REST API to get executor memory metrics.
        This requires Spark UI to be accessible.
        Returns a dict with total physical and virtual memory used by executors (in MB).
        """
        try:
            # Spark UI URL
            spark_ui_url = self.spark.sparkContext.uiWebUrl
            if not spark_ui_url:
                printer.warning("Spark UI URL not found; cannot get executor memory metrics.")
                return None

            executors_api = f"{spark_ui_url}/api/v1/applications/{self.spark.sparkContext.applicationId}/executors"
            response = requests.get(executors_api)
            if response.status_code != 200:
                printer.warning(f"Failed to get executor info: HTTP {response.status_code}")
                return None
            
            executors = response.json()
            
            total_memory_used_mb = 0.0
            total_memory_max_mb = 0.0
            for exe in executors:
                # memory metrics are in bytes 
                # fields: memoryUsed, maxMemory
                memory_used = exe.get('memoryUsed', 0)
                max_memory = exe.get('maxMemory', 0)
                total_memory_used_mb += memory_used / (1024**2)
                total_memory_max_mb += max_memory / (1024**2)

            return {
                'total_memory_used_mb': total_memory_used_mb,
                'total_memory_max_mb': total_memory_max_mb
            }
        except Exception as e:
            printer.warning(f"Exception while fetching executor metrics: {e}")
            return None

    def stop(self):
        """Stop the Spark session."""
        self.spark.stop()

#----------------------------#
#            MAIN            #
#----------------------------#
def main():
    parser = argparse.ArgumentParser(description="Spark Inverted Index Builder with CSV Logging")
    parser.add_argument('--num-partitions', type=int, help="Override number of output partitions")
    parser.add_argument('--limit-mb', type=int, help="Limit input size in MB")
    parser.add_argument(
        '--format', choices=['text', 'json', 'parquet'], default='text',
        help="Output format: text (default), json or parquet"
    )
    args = parser.parse_args()

    # Start measuring execution time
    start_time = time.time()

    engine = InvertedIndexSearch(num_partitions=args.num_partitions)
    try:
        # Select input files
        inputs = choose_input_paths(engine.sc, args.limit_mb)
        if not inputs:
            sys.exit(2)

        printer.info("Spark Inverted Index Builder Application Started ...")

        # Determine output path and its index
        output_path, idx = choose_output_path(engine.sc)

        # Build the index
        engine.build_index(inputs, output_path, output_format=args.format)

        printer.info(f"Index saved to {output_path}")

        # Measure end time and memory usage
        end_time = time.time()
        execution_time = end_time - start_time

        mem_metrics = engine.get_executors_memory_metrics()
        if mem_metrics:
            physical_mb = mem_metrics['total_memory_used_mb']
            virtual_mb  = mem_metrics['total_memory_max_mb']
            printer.info(f"Total Executor Memory Used (MB): {mem_metrics['total_memory_used_mb']:.2f}")
            printer.info(f"Total Executor Max Memory (MB): {mem_metrics['total_memory_max_mb']:.2f}")
        else:
            proc = psutil.Process(os.getpid())
            mem_info = proc.memory_info()
            physical_mb = mem_info.rss / (1024 ** 2)
            virtual_mb  = mem_info.vms / (1024 ** 2)

        printer.info(f"Execution Time: {execution_time}")
        printer.info(f"Total Physical Memory: {physical_mb}")
        printer.info(f"Total Virtual Memory: {virtual_mb}")

        # Write CSV log with same index as output
        log_path = write_log_to_hdfs(engine.sc, idx, execution_time, physical_mb, virtual_mb)
        printer.info(f"Log saved to {log_path}")

        printer.info("Spark Inverted Index Builder Application Ended ...")
    except HDFSPathNotFoundError as hdfse:
        printer.error(f"HDFS error: {hdfse}")
        sys.exit(3)
    except Exception as e:
        printer.exception("Unknown Error: Unexpected error occurred")
        sys.exit(1)
    finally:
        engine.stop()

if __name__ == "__main__":
    main()
