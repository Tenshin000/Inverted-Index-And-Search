# -*- coding: utf-8 -*-
import argparse
import getpass
import os
import re

from recover_resources import download_txt_files
from logs_to_csv_spark import operation_spark
from logs_to_csv_hadoop import operation_hadoop, log_hadoop_reducers
from logs_to_csv_non_parallel import operation_non_parallel
from plot import csv_to_plot
from create_input_partitions import create_partitions

# List of log directories to process
LOG_DIRS = [
    "../log/test-128MB",
    "../log/test-256MB",
    "../log/test-512MB",
    "../log/test-1024MB",
    "../log/test-1583MB",
]

def recover_to_hdfs(step, total):
    print(f"[{step}/{total}] ➡ Recovering resources to HDFS ...")
    ftp_pass = getpass.getpass("Enter your FTP password: ")
    download_txt_files(
        base_url="ftp.blogpanattoni.altervista.org",
        folder_name="resources",
        output_dir="hdfs:///user/hadoop/inverted-index/data",
        ftp_user="blogpanattoni",
        ftp_pass=ftp_pass
    )
    print(f"[{step}/{total}] ✓ Resources recovered to HDFS.")

def recover_to_local(step, total):
    print(f"[{step}/{total}] ➡ Recovering resources locally ...")
    ftp_pass = getpass.getpass("Enter your FTP password: ")
    download_txt_files(
        base_url="ftp.blogpanattoni.altervista.org",
        folder_name="resources",
        output_dir="data",
        ftp_user="blogpanattoni",
        ftp_pass=ftp_pass
    )
    print(f"[{step}/{total}] ✓ Resources recovered locally.")

def process_spark_logs(step_start, total):
    print(f"\n=== [SPARK] Generating CSV from logs ===")
    for i, log_dir in enumerate(LOG_DIRS, start=0):
        step = step_start + i
        print(f"[{step}/{total}] ➡ Processing Spark log: {log_dir}")
        operation_spark(log_dir, "../log/csv-logs/")
        print(f"[{step}/{total}] ✓ Spark CSV generated for: {log_dir}")
    return step_start + len(LOG_DIRS)

def process_hadoop_logs(step_start, total):
    print(f"\n=== [HADOOP] Generating CSV from logs ===")
    for i, log_dir in enumerate(LOG_DIRS, start=0):
        step = step_start + i
        print(f"[{step}/{total}] ➡ Processing Hadoop log: {log_dir}")
        operation_hadoop(log_dir, "../log/csv-logs/")
        print(f"[{step}/{total}] ✓ Hadoop CSV generated for: {log_dir}")
    return step_start + len(LOG_DIRS)

def process_reducer_logs(step, total):
    print(f"[{step}/{total}] ➡ Processing Hadoop reducer logs ...")
    log_hadoop_reducers("../log", "../log/csv-logs/")
    print(f"[{step}/{total}] ✓ Reducer logs processed.")

def process_non_parallel_logs(step_start, total, write_average=False):
    print(f"\n=== [NON-PARALLEL] Generating CSV from logs ===")
    for i, log_dir in enumerate(LOG_DIRS, start=0):
        step = step_start + i
        print(f"[{step}/{total}] ➡ Processing non-parallel logs in: {log_dir}")
        # derive dimension from folder name
        basename = os.path.basename(log_dir)
        match = re.match(r"test-(\d+MB)", basename)
        dim = match.group(1) if match else basename
        # output CSV named log-<dim>-non-parallel.csv
        output_csv = os.path.join("../log/csv-logs", f"log-{dim}-non-parallel.csv")
        operation_non_parallel(log_dir, output_csv, write_average)
        print(f"[{step}/{total}] ✓ Non-parallel CSV generated at: {output_csv}")
    return step_start + len(LOG_DIRS)

def plot_metrics(step, total):
    print(f"[{step}/{total}] ➡ Generating plots from CSV logs ...")
    csv_to_plot()
    print(f"[{step}/{total}] ✓ Plotting completed.")

def create_hdfs_partitions(step, total):
    print(f"[{step}/{total}] ➡ Creating input partitions in HDFS ...")
    create_partitions()
    print(f"[{step}/{total}] ✓ Input partitions created.")

def main():
    parser = argparse.ArgumentParser(description="Inverse Index Search CLI")
    parser.add_argument("--recover-resources", action="store_true",
                        help="Recover files from Altervista and put them in HDFS")
    parser.add_argument("--recover-resources-local", action="store_true",
                        help="Recover files from Altervista and put them locally")
    parser.add_argument("--to-csv-spark", action="store_true",
                        help="Process Spark logs into CSV for all test-* folders")
    parser.add_argument("--to-csv-hadoop", action="store_true",
                        help="Process Hadoop logs into CSV for all test-* folders")
    parser.add_argument("--to-csv-non-parallel", action="store_true",
                        help="Process non-parallel logs into CSV for all test-* folders")
    parser.add_argument("--plot-csv", action="store_true",
                        help="Plot CSV from logs")
    parser.add_argument("--create-partitions", action="store_true",
                        help="Create input partitions from HDFS data folder")

    args = parser.parse_args()

    # Calculate total steps based on selected operations
    total_steps = 0
    if args.recover_resources:
        total_steps += 1
    if args.recover_resources_local:
        total_steps += 1
    if args.to_csv_spark:
        total_steps += len(LOG_DIRS)
    if args.to_csv_hadoop:
        total_steps += len(LOG_DIRS) + 1  # include reducer logs
    if args.to_csv_non_parallel:
        total_steps += len(LOG_DIRS)
    if args.plot_csv:
        total_steps += 1
    if args.create_partitions:
        total_steps += 1
    step_counter = 1

    if args.recover_resources:
        recover_to_hdfs(step_counter, total_steps)
        step_counter += 1

    if args.recover_resources_local:
        recover_to_local(step_counter, total_steps)
        step_counter += 1

    if args.to_csv_spark:
        step_counter = process_spark_logs(step_counter, total_steps)

    if args.to_csv_hadoop:
        step_counter = process_hadoop_logs(step_counter, total_steps)
        process_reducer_logs(step_counter, total_steps)
        step_counter += 1

    if args.to_csv_non_parallel:
        step_counter = process_non_parallel_logs(
            step_counter, total_steps,
            write_average=True
        )

    if args.plot_csv:
        plot_metrics(step_counter, total_steps)
        step_counter += 1

    if args.create_partitions:
        create_hdfs_partitions(step_counter, total_steps)

if __name__ == "__main__":
    main()
