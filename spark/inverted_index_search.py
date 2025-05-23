import argparse
import logging
import os
import sys
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, udf, explode, split, lower, regexp_replace, col, concat_ws, collect_list, concat, lit, sort_array
from pyspark.sql.types import StringType

#----------------------------#
#        CONFIGURATION       #
#----------------------------#
HDFS_BASE                 = 'hdfs:///user/hadoop/'
DATA_DIR                  = HDFS_BASE + 'inverted-index/data'
OUTPUT_BASE               = HDFS_BASE + 'inverted-index/output/'
LOG_DIR                   = HDFS_BASE + 'inverted-index/log/'

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

def choose_input_paths(sc, limit_mb=None, base_dir=DATA_DIR):
    """Efficiently select HDFS input files up to limit_mb, or all if None."""
    if not hdfs_dir_exists(sc, base_dir):
        raise HDFSPathNotFoundError(f"Data directory '{base_dir}' does not exist.")

    if limit_mb is None:
        return [f"{base_dir.rstrip('/')}/"]

    mb_bytes = limit_mb * 1024 * 1024

    # List all files once, sort by descending size to fill the limit faster
    files = sorted(list_hdfs_files(sc, base_dir), key=lambda x: -x[1])

    selected = []
    total = 0

    for path, size in files:
        if total + size <= mb_bytes:
            selected.append(path)
            total += size
        else:
            continue  # try next smaller file

    # If nothing fits (i.e. all files are larger), fall back to smallest-only
    if not selected and files:
        smallest_file = min(files, key=lambda x: x[1])
        selected = [smallest_file[0]]

    return selected

def choose_output_path(sc):
    """Pick a new output directory of form output-sparkX where X increments, in default HDFS."""
    idx = 0
    while hdfs_dir_exists(sc, OUTPUT_BASE + f'output-spark{idx}'):
        idx += 1
    return OUTPUT_BASE + f'output-spark{idx}'

#----------------------------#
#   LOCAL UTILITY FUNCTIONS  #
#----------------------------#
def list_local_txt_files(folder):
    """Recursively list all .txt files under a local folder."""
    paths = []
    for root, dirs, files in os.walk(folder):
        for f in files:
            if f.lower().endswith('.txt'):
                paths.append(os.path.join(root, f))
    return paths

def choose_local_input_paths(paths, limit_mb):
    """Select local files up to limit_mb total size (in MB)."""
    mb_bytes = limit_mb * 1024 * 1024
    selected, total = [], 0
    for path in sorted(paths, key=lambda p: os.path.getsize(p)):
        size = os.path.getsize(path)
        if total + size > mb_bytes:
            break
        selected.append(path)
        total += size
    return selected
    
#----------------------------#
#         EXCEPTION          #
#----------------------------#
class HDFSPathNotFoundError(Exception):
    pass

#----------------------------#
#         SPARK JOB          #
#----------------------------#
class InvertedIndexSearch:
    def __init__(self, app_name="SparkInvertedIndexSearch", num_partitions=None, change_log=False):
        """Configure Spark and initialize SparkSession."""
        conf = SparkConf() \
            .setAppName(app_name) \
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .set("spark.kryoserializer.buffer.max", "512m")
        if num_partitions:
            conf = conf.set("spark.sql.shuffle.partitions", str(num_partitions))
        if change_log:
            conf = conf.set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", LOG_DIR)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext

    def safe_read(self, input_paths):
        """Read all valid text files and attach a 'filename' column."""
        dfs = []

        def safe_read_text(path):
            try:
                return self.spark.read.text(path)
            except Exception as e:
                printer.warning(f"Skipping file {path} due to read error: {e}")
                return None

        for p in input_paths:
            df = safe_read_text(p)
            if df is not None:
                dfs.append(
                    df.withColumn(
                        "filename",
                        udf(lambda x: os.path.basename(x), StringType())(input_file_name())
                    )
                )
        if not dfs:
            raise RuntimeError("No valid input files could be read")
        result = dfs[0]
        for df in dfs[1:]:
            result = result.union(df)
        return result

    def build_index(self, input_paths, output_path, output_format='text'):
        """Build the inverted index and write to output_path."""
        # Phase 1: tokenize
        df = self.safe_read(input_paths)
        tokens = (
            df
            .withColumn('word', explode(split(
                lower(regexp_replace(col('value'), r'[^A-Za-z0-9]', ' ')),
                '\\s+'
            )))
            .filter(col('word') != '')
        )
        # Phase 2: counts and postings
        counts = tokens.groupBy('word', 'filename').count()
        postings = (
            counts
            .select(col('word'),
                    concat(col('filename'), lit(':'), col('count')).alias('posting'))
            .groupBy('word')
            .agg(sort_array(collect_list(col('posting'))).alias('postings_list'))
        )
        # Phase 3: write
        if output_format == 'text':
            formatted = postings.select(
                concat_ws('\t', col('word'),
                          concat_ws('\t', col('postings_list')))
            )
            formatted.write.text(output_path)
        elif output_format == 'json':
            postings.selectExpr("word", "postings_list as docs").write.json(output_path)
        elif output_format == 'parquet':
            postings.selectExpr("word", "postings_list as docs").write.parquet(output_path)
        else:
            formatted = postings.select(
                concat_ws('\t', col('word'),
                          concat_ws('\t', col('postings_list')))
            )
            formatted.write.text(output_path)

    def stop(self):
        """Stop the Spark session."""
        self.spark.stop()

#----------------------------#
#            MAIN            #
#----------------------------#
def main():
    parser = argparse.ArgumentParser(description="Spark Inverted Index Builder")
    parser.add_argument('--num-partitions', type=int, help="Override output partitions")
    parser.add_argument('--limit-mb', type=int, help="Limit input size in MB")
    parser.add_argument(
        '--format', choices=['text', 'json', 'parquet'], default='text',
        help="Output format: text, json, or parquet"
    )
    parser.add_argument('--input-folder', nargs='+', help="Local folders with .txt files")
    parser.add_argument('--input-texts', nargs='+', help="Specific local .txt files")
    parser.add_argument('--input-hdfs-folder', nargs='+',
                        help="HDFS folders under base to read")
    parser.add_argument('--input-hdfs-texts', nargs='+',
                        help="Specific HDFS .txt files under base")
    parser.add_argument('--output', help="Local output folder for results and logs")
    parser.add_argument('--output-hdfs',
                        help="HDFS output folder under base for results and logs")
    args = parser.parse_args()

    change_log = True
    if args.output:
        change_log = False
    elif args.output_hdfs:
        change_log = False

    engine = InvertedIndexSearch(num_partitions=args.num_partitions,change_log=change_log)
    try:
        printer.info("Spark Inverted Index Builder Application Started ...")
        start_time = time.time()
        use_local_output = bool(args.output)

        # Determine input paths
        input_paths = []
        if args.input_folder:
            for folder in args.input_folder:
                files = list_local_txt_files(folder)
                if args.limit_mb is not None:
                    input_paths.extend(choose_local_input_paths(files, args.limit_mb))
                else:
                    input_paths.extend(files)
        if args.input_texts:
            input_paths.extend(args.input_texts)
        if args.input_hdfs_folder:
            for hf in args.input_hdfs_folder:
                hdfs_path = HDFS_BASE + hf.rstrip('/')
                if args.limit_mb is not None:
                    input_paths.extend(choose_input_paths(engine.sc, args.limit_mb, hdfs_path))
                else:
                    input_paths.append(hdfs_path + '/*')
        if args.input_hdfs_texts:
            for ht in args.input_hdfs_texts:
                input_paths.append(HDFS_BASE + ht)
        if not input_paths:
            input_paths = choose_input_paths(engine.sc, args.limit_mb)

        # Determine output path
        if use_local_output:
            output_path = args.output
            os.makedirs(output_path, exist_ok=True)
        elif args.output_hdfs:
            base = HDFS_BASE + args.output_hdfs.rstrip('/')
            idx = 0
            while hdfs_dir_exists(engine.sc, f"{base}-spark{idx}"):
                idx += 1
            output_path = f"{base}-spark{idx}"
        else:
            output_path = choose_output_path(engine.sc)

        # Build the index
        engine.build_index(input_paths, output_path, output_format=args.format)
        printer.info(f"Index saved to {output_path}")

        end_time = time.time()
        printer.info(f"Execution Time: {(end_time - start_time):.3f} seconds")
        printer.info("Spark Inverted Index Builder Application Finished ...")

    except HDFSPathNotFoundError as hdfse:
        printer.error(f"HDFS error: {hdfse}")
        sys.exit(2)
    except Exception as e:
        printer.exception("Unknown Error: Unexpected error occurred")
        sys.exit(1)
    finally:        
        engine.stop()

if __name__ == "__main__":
    main()