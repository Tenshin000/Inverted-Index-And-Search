import argparse
from collections import Counter
import logging
import os
import psutil
import re
import requests
import sys
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession


#----------------------------#
#        CONFIGURATION       #
#----------------------------#
HDFS_BASE                 = 'hdfs:///user/hadoop/'
DATA_DIR                  = HDFS_BASE + 'inverted-index/data'
OUTPUT_BASE               = HDFS_BASE + 'inverted-index/output/'
LOG_DIR                   = HDFS_BASE + 'inverted-index/log/'
MAX_TASKS                 = 500

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
    
    def is_clean(path):
        name = os.path.basename(path)
        base, ext = os.path.splitext(name)
        return "," not in base and "." not in base and ext == ".txt"

    all_files = list_hdfs_files(sc, base_dir)
    clean_files = [(f, sz) for f, sz in all_files if is_clean(f)]

    # List all files once, sort by descending size to fill the limit faster
    files = sorted(clean_files, key=lambda x: -x[1])

    if limit_mb is None:
        return [f for f, _ in files]

    mb_bytes = limit_mb * 1024 * 1024    

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
    # Fields
    spark: SparkSession
    sc: SparkConf
    num_partitions: int

    # Methods
    def __init__(self, app_name="RDDSparkInvertedIndexSearch", num_partitions=None):
        """Configure Spark and initialize SparkSession."""
        conf = SparkConf() \
                .setAppName(app_name) \
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .set("spark.kryoserializer.buffer.max", "512m") \
                .set("spark.speculation", "true") \
                .set("spark.speculation.multiplier", "1.5") \
                .set("spark.eventLog.enabled", "true") \
                .set("spark.dynamicAllocation.enabled", "true") \
                .set("spark.dynamicAllocation.minExecutors", "3") \
                .set("spark.dynamicAllocation.initialExecutors", "3")
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext
        if num_partitions is not None:
            self.num_partitions = num_partitions
        else:
            self.num_partitions = None

    def build_index(self, input_paths, output_path, output_format="text"):
        """Build inverted index from text files and save in specified format."""

        # Ensure input_paths is a list
        if isinstance(input_paths, str):
            input_paths = [input_paths]

        # Reads each file in path returning an RDD of (filePath, fileContent) pairs
        rdds = [self.sc.wholeTextFiles(path) for path in input_paths]
        # Joins all read files into a single RDD, without shuffling (narrow)
        files_rdd = self.sc.union(rdds)

        # Tokenize and count words per document
        def tokenize_and_count(pair):
            """Grouping the count per document here avoids sending single occurrences word-by-word to the cluster, """
            """reducing the number of lines emitted (less network overhead)."""
            path, text = pair
            # Take the filename
            basename = os.path.basename(path)
            # Converts text to lowercase and uses regex to take only words and numbers
            words = re.findall(r"\b\w+\b", text.lower().replace("_", " "))
            # Builds a local Counter to count the occurrences of each word in that document
            counts = Counter(words)
            # Returns a list of tuples (word, (filename, count))
            return [(word, {basename: cnt}) for word, cnt in counts.items()]

        tasks = MAX_TASKS
        # Take a list of ((word, filename), count)
        intermediate = files_rdd.coalesce(tasks).flatMap(tokenize_and_count)
        
        tasks = MAX_TASKS // 4
        
        postings = intermediate.coalesce(tasks).aggregateByKey(
            zeroValue={},  # empty dict as starting point
            # seqFunc: adds dict d to dict acc
            seqFunc=lambda acc, d: {**acc, **{k: acc.get(k, 0) + d[k] for k in d}},
            # combFunc: merges two dicts from different partitions
            combFunc=lambda acc1, acc2: {**acc1, **{k: acc1.get(k, 0) + acc2[k] for k in acc2}}
        )

        # Format output as tab-separated lines
        formatted = postings.map(
            lambda wc: wc[0] + "\t" + "\t".join(f"{fname}:{cnt}" for fname, cnt in wc[1].items())
        )

        # Repartition or coalesce if needed
        if self.num_partitions is not None:
            final_rdd = (formatted.coalesce(1) if self.num_partitions <= 1 
                        else formatted.repartition(self.num_partitions))
        else:
            final_rdd = formatted

        # Save output
        if output_format == "text":
            final_rdd.saveAsTextFile(output_path)
        else:
            df = self.spark.createDataFrame(
                final_rdd.map(lambda line: line.split("\t", 1))
                        .map(lambda parts: {"word": parts[0], "postings": parts[1]})
            )
            writer = df.coalesce(1) if self.num_partitions == 1 else (
                    df.repartition(self.num_partitions) if self.num_partitions else df)
            mode = "overwrite"
            if output_format == "json":
                writer.write.mode(mode).json(output_path)
            elif output_format == "parquet":
                writer.write.mode(mode).parquet(output_path)
            else:
                final_rdd.saveAsTextFile(output_path)

    def get_hdfs_dir_size(self, path):
        """Compute total size of all files in an HDFS path."""
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

    def collect_and_log_metrics(self, log_dir=None, output_path=None, execution_time="Error"):
        """Collect Spark and system metrics and log them."""
        log_lines = []

        def log_and_store(message):
            printer.info(message)
            log_lines.append(message)

        output_size_bytes = self.get_hdfs_dir_size(output_path)
        output_size_mb = output_size_bytes / (1024 ** 2)

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

        app_id = self.sc.applicationId
        host = self.sc._conf.get("spark.driver.host")
        port = self.sc._conf.get("spark.ui.port", "4040")
        url = f"http://{host}:{port}/api/v1/applications/{app_id}/executors"
        execs = requests.get(url).json()

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
            "ProcessTreeOtherVMemory": 0
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
                if(peak_stage_b <= metrics.get("peakExecutionMemory", 0)):
                    peak_stage_b = metrics.get("peakExecutionMemory", 0)
                stage_task_duration_ms += metrics.get("executorRunTime", 0)
                stage_memory_spilled_b += metrics.get("memoryBytesSpilled", 0)
                stage_disk_spilled_b += metrics.get("diskBytesSpilled",0)

        physical_mem_snapshot_mb = (
            executor_agg["ProcessTreeJVMRSSMemory"] +
            executor_agg["ProcessTreePythonRSSMemory"] +
            executor_agg["ProcessTreeOtherRSSMemory"]
        ) / MB

        virtual_mem_snapshot_mb = (
            executor_agg["ProcessTreeJVMVMemory"] +
            executor_agg["ProcessTreePythonVMemory"] +
            executor_agg["ProcessTreeOtherVMemory"]
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
        stage_memory_spilled_mb = stage_memory_spilled_b  / MB 
        stage_disk_spilled_mb = stage_disk_spilled_b / MB
        total_stage_cpu_s = stage_cpu_time_ns * NS_TO_S
        total_stage_peak_mb = stage_peak_memory / MB
        duration_s = stage_task_duration_ms / 1000.0
        gc_time_s = executor_agg["totalGCTime"] / 1000.0

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

        log_name = f"log-{os.path.basename(output_path)}"

        # Writing the log file
        if log_dir is None:
            log_dir = LOG_DIR

        if log_dir.startswith("hdfs://"):
            # Write to HDFS using Spark
            df = self.spark.createDataFrame([(l,) for l in log_lines], ["log"])
            hdfs_log_path = os.path.join(log_dir, log_name)
            df.coalesce(1).write.mode("overwrite").text(hdfs_log_path)
            printer.info(f"Log saved to HDFS path: {hdfs_log_path}")
        else:
            # Write to local filesystem
            log_name = log_name + ".log"
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, log_name)
            with open(log_path, "w") as f:
                for line in log_lines:
                    f.write(line + "\n")
            printer.info(f"Log saved to local path: {log_path}")

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
    parser.add_argument('--input-hdfs-folder', nargs='+', help="HDFS folders under base to read")
    parser.add_argument('--input-hdfs-texts', nargs='+', help="Specific HDFS .txt files under base")
    parser.add_argument('--output', help="Local output folder for results and logs")
    parser.add_argument('--output-hdfs', help="HDFS output folder under base for results and logs")
    parser.add_argument('--log-local', help="Local folder to save log file")
    parser.add_argument('--log-hdfs', help="HDFS folder to save log file")
    args = parser.parse_args()

    start_time = time.time()
    engine = InvertedIndexSearch(num_partitions=args.num_partitions)
    try:
        printer.info("Spark Inverted Index Builder Application Started ...")
        
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

        # Determine log path
        if args.log_local:
            log_dir = args.log_local
        elif args.log_hdfs:
            log_dir = HDFS_BASE + args.log_hdfs.rstrip('/')
        else:
            log_dir = LOG_DIR
        
        # Build the index
        engine.build_index(input_paths, output_path)
        printer.info(f"Index saved to {output_path}")
        end_time = time.time()
        execution_time = end_time - start_time
        # Collect the statistics
        engine.collect_and_log_metrics(log_dir, output_path, execution_time)
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
