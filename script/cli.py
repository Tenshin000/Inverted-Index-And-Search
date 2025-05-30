import argparse
import getpass
import os
import importlib.util
import sys

from recover_resources import download_txt_files
from logs_to_csv_spark import operation_spark
from logs_to_csv_hadoop import operation_hadoop
from plot import csv_to_plot

# List of log directories to process
LOG_DIRS = [
    "../log/test-128MB",
    "../log/test-256MB",
    "../log/test-512MB",
    "../log/test-1024MB",
    "../log/test-1583MB",
]

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
    parser.add_argument("--plot-csv", action="store_true",
                        help="Plot csv from log folders filtering by given technologies")

    args = parser.parse_args()

    # BLOCK 1: recover resources into HDFS
    if args.recover_resources:
        total = 1
        step = 1
        print(f"[{step}/{total}] ➡ Resource recovery to HDFS …")
        ftp_pass = getpass.getpass("Enter your FTP password: ")
        download_txt_files(
            base_url="ftp.blogpanattoni.altervista.org",
            folder_name="resources",
            output_dir="hdfs:///user/hadoop/inverted-index/data",
            ftp_user="blogpanattoni",
            ftp_pass=ftp_pass
        )
        print(f"[{step}/{total}] ✓ Resources recovered to HDFS.")

    # BLOCK 2: recover resources locally
    if args.recover_resources_local:
        total = 1
        step = 1
        print(f"[{step}/{total}] ➡ Resource recovery locally …")
        ftp_pass = getpass.getpass("Enter your FTP password: ")
        download_txt_files(
            base_url="ftp.blogpanattoni.altervista.org",
            folder_name="resources",
            output_dir="data",
            ftp_user="blogpanattoni",
            ftp_pass=ftp_pass
        )
        print(f"[{step}/{total}] ✓ Resources recovered locally.")

    # BLOCK 3: process Spark logs
    if args.to_csv_spark:
        total = len(LOG_DIRS)
        print(f"\n=== SPARK: Generating CSV from logs ({total} folders) ===")
        for idx, log_dir in enumerate(LOG_DIRS, start=1):
            print(f"[{idx}/{total}] ➡ Running Spark CSV operation on {log_dir} …")
            operation_spark(log_dir, "../log/csv-logs/")
            print(f"[{idx}/{total}] ✓ Spark CSV operation completed for {log_dir}")

    # BLOCK 4: process Hadoop logs
    if args.to_csv_hadoop:
        total = len(LOG_DIRS)
        print(f"\n=== HADOOP: Generating CSV from logs ({total} folders) ===")
        for idx, log_dir in enumerate(LOG_DIRS, start=1):
            print(f"[{idx}/{total}] ➡ Running Hadoop CSV operation on {log_dir} …")
            operation_hadoop(log_dir, "../log/csv-logs/")
            print(f"[{idx}/{total}] ✓ Hadoop CSV operation completed for {log_dir}")

    # BLOCK 5: plot logs
    if args.plot_csv:
        technologies = ["spark", "hadoop", "rdd-spark", "noimc-hadoop"]

        print(f"\n=== PLOTTING: Plotting metrics for technologies {technologies} in {len(LOG_DIRS)} folders ===")
        csv_to_plot()
        print("✓ Plotting completed")                     

if __name__ == "__main__":
    main()
