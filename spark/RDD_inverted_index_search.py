import argparse
import json
import logging
import os
from os.path import basename
import psutil
import re
import requests
import sys
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

# ----------------------------#
#        CONFIGURATIONS       #
# ----------------------------# 
HDFS_BASE = "hdfs:///user/hadoop/"
DATA_DIR = HDFS_BASE + "inverted-index/data"
OUTPUT_BASE = HDFS_BASE + "inverted-index/output/"
LOG_DIR = HDFS_BASE + "inverted-index/log/"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
printer = logging.getLogger(__name__)


# ----------------------------#
#          EXCEPTION          #
# ----------------------------# 
class HDFSPathNotFoundError(Exception):
    pass


# ----------------------------#
#      SPARK APPLICATION      #
# ----------------------------# 
class InvertedIndexSearch:
    # ----------------------------#
    #            Fields           #
    # ----------------------------#
    spark: SparkSession
    sc: SparkConf
    num_partitions: int

    # ----------------------------#
    #       Standard Methods      #
    # ----------------------------#
    def __init__(self, app_name="SparkInvertedIndexSearchRDD", num_partitions=None):
        """Configure Spark and initialize SparkSession."""
        conf = (
            SparkConf()
            .setAppName(app_name)
        )
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext
        self.num_partitions = num_partitions if num_partitions is not None else None
        printer.info("Spark Inverted Index Application Started ...")

    def stop(self):
        """Stop the Spark session."""
        printer.info("Spark Inverted Index Application Finished ...")
        self.spark.stop()

    # ----------------------------#
    #     HDFS Utilty Methods     #
    # ----------------------------#
    def hdfs_dir_exists(self, path: str) -> bool:
        """Check if an HDFS path exists."""
        jvm = self.sc._jvm
        hadoop_conf = self.sc._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        return fs.exists(jvm.org.apache.hadoop.fs.Path(path))

    def list_hdfs_files(self, path: str) -> list:
        """List all files (path, size) in an HDFS directory (non-recursive)."""
        jvm = self.sc._jvm
        hadoop_conf = self.sc._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        p = jvm.org.apache.hadoop.fs.Path(path)
        files = []
        if fs.exists(p):
            for status in fs.listStatus(p):
                if status.isFile():
                    files.append((status.getPath().toString(), status.getLen()))
        return files

    def choose_input_paths(self, limit_mb: int = None, base_dir: str = DATA_DIR) -> list:
        """Select HDFS input paths up to limit_mb (MB), or all if limit_mb is None."""
        if not self.hdfs_dir_exists(base_dir):
            raise HDFSPathNotFoundError(f"Data directory '{base_dir}' does not exist.")

        if limit_mb is None:
            # Read entire directory
            return [f"{base_dir.rstrip('/')}/"]

        mb_bytes = limit_mb * 1024 * 1024

        # List all files once, sort by descending size
        files = sorted(self.list_hdfs_files(base_dir), key=lambda x: -x[1])

        selected = []
        total = 0

        for path, size in files:
            if total + size <= mb_bytes:
                selected.append(path)
                total += size
            else:
                continue

        # If nothing fits, pick the smallest file
        if not selected and files:
            smallest_file = min(files, key=lambda x: x[1])
            selected = [smallest_file[0]]

        return selected

    def choose_output_path(self) -> str:
        """Pick a new output directory of form output-sparkX under OUTPUT_BASE."""
        idx = 0
        while self.hdfs_dir_exists(OUTPUT_BASE + f"output-spark{idx}"):
            idx += 1
        return OUTPUT_BASE + f"output-spark{idx}"
    
    def get_hdfs_dir_size(self, path: str) -> int:
        """Recursively compute total size (in bytes) of all files under an HDFS path."""
        jvm = self.sc._jvm
        hadoop_conf = self.sc._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        p = jvm.org.apache.hadoop.fs.Path(path)
        total_size = 0
        if fs.exists(p):
            for status in fs.listStatus(p):
                if status.isFile():
                    total_size += status.getLen()
                elif status.isDirectory():
                    total_size += self.get_hdfs_dir_size(status.getPath().toString())
        return total_size   

    def get_hdfs_file_size(self, path: str) -> int:
        """Get size of a single HDFS file (in bytes)."""
        jvm = self.sc._jvm
        hadoop_conf = self.sc._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        p = jvm.org.apache.hadoop.fs.Path(path)
        if fs.exists(p) and fs.isFile(p):
            return fs.getFileStatus(p).getLen()
        return 0

    # ----------------------------#
    #          Spark Job          #
    # ----------------------------#
    def build_index(self, input_paths: list, output_path: str, output_format="text"):
        """Build the inverted index and write to HDFS at output_path."""
        # Read all files as (fullFilePath, content) pairs
        # We accumulate them via union(...) so that arbitrary lists of files / directories work.

        rdd = None

        if self.num_partitions is None:
            total_input_bytes = 0

            for path in input_paths:
                if self.hdfs_dir_exists(path):
                    if path.endswith("/"):
                        # Directory
                        total_input_bytes += self.get_hdfs_dir_size(path)
                    else:
                        # File
                        total_input_bytes += self.get_hdfs_file_size(path)

            self.num_partitions = max(1, total_input_bytes // (44 * 1024 * 1024)) + 1

        # From (fullPath, content), produce a flat sequence of (term, docID) for each occurrence.
        # We will lowercase and remove any non-alphanumeric characters.
        def tokenize_file(pair):
            """
            pair: (fullPath: String, content: String)
            returns an iterator of ((term, docID), 1) for every occurrence of 'term' in that document
            """
            fullpath, text = pair
            # Extract just the base filename (e.g. "hdfs:///.../doc1.txt" -> "doc1.txt")
            docID = basename(fullpath)

            # Let's convert everything to lowercase and replace non-alphanumeric characters/spaces with space
            cleaned = re.sub(r"[^\w\s]", " ", text.lower())

            # Now we can split on spaces to get the words
            for word in cleaned.split():
                # Let's avoid empty words
                if word:
                    yield ((word, docID), 1)

        # For consistent ordering, sort by term (key) and also sort each list of postings by docID:
        def sort_postings(record):
            """
            record: (term: String, postings_iter: Iterable[(docID, count)])
            returns (term, List[(docID, count)] sorted by docID string)
            """
            term, postings_iter = record
            postings_list = list(postings_iter)
            postings_list.sort(key=lambda x: x[0])  # sort by docID string
            return (term, postings_list)

        for path in input_paths:
            # wholeTextFiles --> RDD[(String path, String content)]
            this_rdd = self.sc.wholeTextFiles(path)
            if rdd is None:
                rdd = this_rdd
            else:
                rdd = rdd.union(this_rdd)
        rdd = rdd.repartition(self.num_partitions)

        # If no files matched (e.g. input_paths was empty), we just return early:
        if rdd is None:
            # Create an empty directory at output_path to avoid downstream errors
            self.sc.parallelize([], 1).saveAsTextFile(output_path)
            return

        # flatMap -> ((term, docID), 1)
        token_pairs = rdd.flatMap(tokenize_file)

        # Sum up counts per (term,docID): reduceByKey(+) -> ((term, docID), count)
        counts = token_pairs.reduceByKey(lambda a, b: a + b, self.num_partitions)

        # Now transform to (term, (docID, count)) and group by term
        postings = counts.map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
        # postings: RDD[(term, (docID, count))]

        # Group all (docID, count) pairs per term:
        grouped = postings.groupByKey()
        # grouped: RDD[(term, Iterable[(docID, count)])]

        # sorted_index: RDD[(term, List[(docID, count)])], with terms in ascending lex order
        sorted_index = grouped.map(sort_postings).sortByKey()
        # sorted_index = sorted_index.cache()
        
        # Depending on output_format, write out differently:
        fmt = output_format.lower()
        if fmt not in ("text", "json", "parquet"):
            fmt = "text"

        if fmt == "text":
            # Build one line per term:
            #   "word \t docA:countA \t docB:countB \t "
            def to_text_line(record):
                term, postings_list = record
                # Build "docA:countA" strings
                posting_strs = [f"{doc}:{cnt}" for (doc, cnt) in postings_list]
                # Join by tabs
                return term + "\t" + "\t".join(posting_strs)

            lines = sorted_index.map(to_text_line)
            lines.saveAsTextFile(output_path)

        elif fmt == "json":
            # Each record -> JSON string:
            def to_json_line(record):
                term, postings_list = record
                # Build array of JSON objects
                arr = [{"doc": doc, "count": cnt} for (doc, cnt) in postings_list]
                # Build a dictionary and dump to JSON
                obj = {"word": term, "postings": arr}
                return json.dumps(obj)

            json_lines = sorted_index.map(to_json_line)
            # Just write out one JSON object per line:
            json_lines.saveAsTextFile(output_path)

        elif fmt == "parquet":  
            # Convert each (term, postings_list) into a Row for a DataFrame:
            #   Row(word=term, postings=[Row(doc=..., count=...), ...])
            def to_row(record):
                term, postings_list = record
                # Build a list of Row(doc=..., count=...)
                posting_rows = [Row(doc=doc, count=cnt) for (doc, cnt) in postings_list]
                return Row(word=term, postings=posting_rows)

            rows_rdd = sorted_index.map(to_row)
            df = self.spark.createDataFrame(rows_rdd)
            # Write out in Parquet format
            df.write.mode("overwrite").parquet(output_path)
        
        else:
            #   "word \t docA:countA \t docB:countB \t "
            def to_text_line(record):
                term, postings_list = record
                # Build "docA:countA" strings
                posting_strs = [f"{doc}:{cnt}" for (doc, cnt) in postings_list]
                # Join by tabs
                return term + "\t" + "\t".join(posting_strs)

            lines = sorted_index.map(to_text_line)
            lines.saveAsTextFile(output_path)

    # ----------------------------#
    #          Statistics         #
    # ----------------------------#
    def collect_and_log_metrics(self, log_dir: str, output_path: str, execution_time: float):
        """Collect Spark/executor metrics + driver metrics, then write a log file to HDFS."""
        log_lines = []

        def log_and_store(message: str):
            printer.info(message)
            log_lines.append(message)

        # Compute output size on HDFS
        output_size_bytes = self.get_hdfs_dir_size(output_path)
        output_size_mb = output_size_bytes / (1024 ** 2)

        # Driver metrics via psutil
        proc = psutil.Process()
        driver_rss_b = proc.memory_info().rss
        driver_cpu_time = sum(proc.cpu_times())
        disk_counters = psutil.disk_io_counters()
        driver_disk_read_b = disk_counters.read_bytes
        driver_disk_write_b = disk_counters.write_bytes

        MB = 1024 ** 2
        NS_TO_S = 1e-9

        driver_rss_mb = driver_rss_b / MB
        driver_disk_read_mb = driver_disk_read_b / MB
        driver_disk_write_mb = driver_disk_write_b / MB

        # Fetch Spark executor metrics via Spark REST API
        app_id = self.sc.applicationId
        host = self.sc._conf.get("spark.driver.host")
        port = self.sc._conf.get("spark.ui.port", "4040")
        executors_url = f"http://{host}:{port}/api/v1/applications/{app_id}/executors?include=dead=true"
        execs = requests.get(executors_url).json()

        executor_agg = {
            "rddBlocks": 0,
            "memoryUsed": 0,
            "maxMemory": 0,
            "diskUsed": 0,
            "totalTasks": 0,
            "completedTasks": 0,
            "totalGCTime": 0,
            "totalInputBytes": 0,
            "totalShuffleRead": 0,
            "totalShuffleWrite": 0,
            "onHeapExecMem": 0,
            "offHeapExecMem": 0,
            "ProcessTreeJVMRSSMemory": 0,
            "ProcessTreePythonRSSMemory": 0,
            "ProcessTreeOtherRSSMemory": 0,
            "ProcessTreeJVMVMemory": 0,
            "ProcessTreePythonVMemory": 0,
            "ProcessTreeOtherVMemory": 0,
        }

        for e in execs:
            executor_agg["rddBlocks"] += e.get("rddBlocks", 0)
            executor_agg["memoryUsed"] += e.get("memoryUsed", 0)
            executor_agg["maxMemory"] += e.get("maxMemory", 0)
            executor_agg["diskUsed"] += e.get("diskUsed", 0)
            executor_agg["totalTasks"] += e.get("totalTasks", 0)
            executor_agg["completedTasks"] += e.get("completedTasks", 0)
            executor_agg["totalGCTime"] += e.get("totalGCTime", 0)
            executor_agg["totalInputBytes"] += e.get("totalInputBytes", 0)
            executor_agg["totalShuffleRead"] += e.get("totalShuffleRead", 0)
            executor_agg["totalShuffleWrite"] += e.get("totalShuffleWrite", 0)
            peak = e.get("peakMemoryMetrics", {})
            executor_agg["onHeapExecMem"] += peak.get("OnHeapExecutionMemory", 0)
            executor_agg["offHeapExecMem"] += peak.get("OffHeapExecutionMemory", 0)
            executor_agg["ProcessTreeJVMRSSMemory"] += peak.get("ProcessTreeJVMRSSMemory", 0)
            executor_agg["ProcessTreePythonRSSMemory"] += peak.get("ProcessTreePythonRSSMemory", 0)
            executor_agg["ProcessTreeOtherRSSMemory"] += peak.get("ProcessTreeOtherRSSMemory", 0)
            executor_agg["ProcessTreeJVMVMemory"] += peak.get("ProcessTreeJVMVMemory", 0)
            executor_agg["ProcessTreePythonVMemory"] += peak.get("ProcessTreePythonVMemory", 0)
            executor_agg["ProcessTreeOtherVMemory"] += peak.get("ProcessTreeOtherVMemory", 0)

        # Fetch per-task metrics from each stage
        stages_url = f"http://{host}:{port}/api/v1/applications/{app_id}/stages"
        stages = requests.get(stages_url).json()

        stage_cpu_time_ns = 0
        stage_peak_memory = 0
        stage_task_duration_ms = 0
        stage_memory_spilled_b = 0
        stage_disk_spilled_b = 0
        peak_stage_b = 0

        for stage in stages:
            sid = stage.get("stageId")
            attempt = stage.get("attemptId", 0)
            task_url = f"http://{host}:{port}/api/v1/applications/{app_id}/stages/{sid}/{attempt}/taskList"
            task_data = requests.get(task_url).json()

            for task in task_data:
                metrics = task.get("taskMetrics", {})
                stage_cpu_time_ns += metrics.get("executorCpuTime", 0)
                stage_peak_memory += metrics.get("peakExecutionMemory", 0)
                if peak_stage_b <= metrics.get("peakExecutionMemory", 0):
                    peak_stage_b = metrics.get("peakExecutionMemory", 0)
                stage_task_duration_ms += metrics.get("executorRunTime", 0)
                stage_memory_spilled_b += metrics.get("memoryBytesSpilled", 0)
                stage_disk_spilled_b += metrics.get("diskBytesSpilled", 0)

        # Compute aggregated memory snapshots
        physical_mem_snapshot_mb = (
            executor_agg["ProcessTreeJVMRSSMemory"]
            + executor_agg["ProcessTreePythonRSSMemory"]
            + executor_agg["ProcessTreeOtherRSSMemory"]
        ) / MB

        virtual_mem_snapshot_mb = (
            executor_agg["ProcessTreeJVMVMemory"]
            + executor_agg["ProcessTreePythonVMemory"]
            + executor_agg["ProcessTreeOtherVMemory"]
        ) / MB

        rdd_blocks = executor_agg["rddBlocks"]
        memory_used_mb = executor_agg["memoryUsed"] / MB
        max_memory_used_mb = executor_agg["maxMemory"] / MB
        disk_used_mb = executor_agg["diskUsed"] / MB
        total_tasks = executor_agg["totalTasks"]
        completed_tasks = executor_agg["completedTasks"]
        hdfs_read_mb = executor_agg["totalInputBytes"] / MB
        shuffle_read_mb = executor_agg["totalShuffleRead"] / MB
        shuffle_write_mb = executor_agg["totalShuffleWrite"] / MB
        on_heap_exec_mb = executor_agg["onHeapExecMem"] / MB
        off_heap_exec_mb = executor_agg["offHeapExecMem"] / MB
        total_exec_mem_mb = (executor_agg["onHeapExecMem"] + executor_agg["offHeapExecMem"]) / MB
        peak_stage_mb = peak_stage_b / MB
        stage_memory_spilled_mb = stage_memory_spilled_b / MB
        stage_disk_spilled_mb = stage_disk_spilled_b / MB
        total_stage_cpu_s = stage_cpu_time_ns * NS_TO_S
        total_stage_peak_mb = stage_peak_memory / MB
        duration_s = stage_task_duration_ms / 1000.0
        gc_time_s = executor_agg["totalGCTime"] / 1000.0

        # Log all metrics
        log_and_store(f"App ID                        : {app_id}")
        log_and_store(f"Execution Time                : {execution_time:.3f} seconds")
        log_and_store(f"Total tasks launched          : {total_tasks}")
        log_and_store(f"Tasks completed               : {completed_tasks}")
        log_and_store(f"Total tasks duration          : {duration_s:.3f} seconds")
        log_and_store(f"Physical Memory Snapshot      : {physical_mem_snapshot_mb:.2f} MB")
        log_and_store(f"Virtual Memory Snapshot       : {virtual_mem_snapshot_mb:.2f} MB")
        log_and_store(f"Driver CPU time               : {driver_cpu_time:.3f} seconds")
        log_and_store(f"Total CPU time                : {total_stage_cpu_s:.3f} seconds")
        log_and_store(f"Total GC time                 : {gc_time_s:.3f} seconds")
        log_and_store(f"Driver RSS memory             : {driver_rss_mb:.2f} MB")
        log_and_store(f"Executor Memory used          : {memory_used_mb:.2f} MB")
        log_and_store(f"Executor Max Memory           : {max_memory_used_mb:.2f} MB")
        log_and_store(f"Total Peak execution memory   : {total_exec_mem_mb:.2f} MB")
        log_and_store(f"  - On heap                   : {on_heap_exec_mb:.2f} MB")
        log_and_store(f"  - Off heap                  : {off_heap_exec_mb:.2f} MB")
        log_and_store(f"Total Peak Stage memory       : {total_stage_peak_mb:.2f} MB")
        log_and_store(f"Peak Stage Memory             : {peak_stage_mb:.2f} MB")
        log_and_store(f"Disk used for RDD             : {disk_used_mb:.2f} MB")
        log_and_store(f"Driver disk read              : {driver_disk_read_mb:.2f} MB")
        log_and_store(f"Driver disk write             : {driver_disk_write_mb:.2f} MB")
        log_and_store(f"HDFS read                     : {hdfs_read_mb:.2f} MB")
        log_and_store(f"HDFS written                  : {output_size_mb:.2f} MB")
        log_and_store(f"Shuffle read                  : {shuffle_read_mb:.2f} MB")
        log_and_store(f"Shuffle write                 : {shuffle_write_mb:.2f} MB")
        log_and_store(f"Memory Spilled                : {stage_memory_spilled_mb:.2f} MB")
        log_and_store(f"Disk Spilled                  : {stage_disk_spilled_mb:.2f} MB")
        log_and_store(f"RDD blocks cached             : {rdd_blocks}")

        # Prepare log file name
        log_name = f"log-{os.path.basename(output_path)}"

        # Write the log to HDFS
        df = self.spark.createDataFrame([(l,) for l in log_lines], ["log"])
        hdfs_log_path = os.path.join(log_dir.rstrip("/"), log_name)
        df.coalesce(1).write.mode("overwrite").text(hdfs_log_path)
        printer.info(f"Log saved to HDFS path: {hdfs_log_path}")   


# ----------------------------#
#            MAIN            #
# ----------------------------#
def main():
    parser = argparse.ArgumentParser(description="Spark Inverted Index Builder")
    parser.add_argument(
        "--num-partitions", type=int, help="Override number of output partitions"
    )
    parser.add_argument(
        "--limit-mb", type=int, help="Limit total input size (in MB) from HDFS"
    )
    parser.add_argument(
        "--format",
        choices=["text", "json", "parquet"],
        default="text",
        help="Output format: text, json, or parquet",
    )
    parser.add_argument(
        "--input-folder",
        nargs="+",
        help="HDFS directories under hdfs:///user/hadoop/ to read from",
    )
    parser.add_argument(
        "--input-texts",
        nargs="+",
        help="Specific HDFS text files (relative to base) to read, e.g. 'inverted-index/data/file1.txt'",
    )
    parser.add_argument(
        "--output",
        help="HDFS sub-directory under base to write results (will be suffixed with -sparkX)",
    )
    parser.add_argument(
        "--log",
        help="HDFS folder under base to save log file (defaults to inverted-index/log)",
    )
    args = parser.parse_args()

    start_time = time.time()
    engine = InvertedIndexSearch(num_partitions=args.num_partitions)
    try:
        input_paths = []
        if args.input_folder:
            for hf in args.input_folder:
                # Compose full HDFS path to the folder
                hdfs_path = HDFS_BASE + hf.rstrip("/")
                if args.limit_mb is not None:
                    # Pick individual files up to limit
                    selected = engine.choose_input_paths(args.limit_mb, hdfs_path)
                    input_paths.extend(selected)
                else:
                    # Read entire folder
                    input_paths.append(f"{hdfs_path.rstrip('/')}/")

        if args.input_texts:
            for ht in args.input_texts:
                input_paths.append(HDFS_BASE + ht)

        if not input_paths:
            # If no explicit HDFS inputs provided, read from DATA_DIR
            if args.limit_mb is not None:
                input_paths = engine.choose_input_paths(args.limit_mb, DATA_DIR)
            else:
                input_paths = [f"{DATA_DIR.rstrip('/')}/"]
        
        if args.output:
            base = HDFS_BASE + args.output.rstrip("/")
            idx = 0
            while engine.hdfs_dir_exists(f"{base}-spark{idx}"):
                idx += 1
            output_path = f"{base}-spark{idx}"
        else:
            # Auto-increment under OUTPUT_BASE
            output_path = engine.choose_output_path()

        if args.log:
            log_dir = HDFS_BASE + args.log.rstrip("/")
        else:
            log_dir = LOG_DIR
        
        engine.build_index(input_paths, output_path, output_format=args.format)
        printer.info(f"Index saved to HDFS at: {output_path}")

        end_time = time.time()
        execution_time = end_time - start_time

        engine.collect_and_log_metrics(log_dir, output_path, execution_time)

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
