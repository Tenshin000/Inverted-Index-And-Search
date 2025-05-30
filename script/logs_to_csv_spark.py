import csv
import os
import re
import statistics
import sys

def extract_mb_from_path(path):
    basename = os.path.basename(path)
    match = re.search(r'(\d+)MB', basename, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        return "unknown"

def extract_value(line, key):
    """Extracts a numeric value from a log line based on the provided key."""
    pattern = rf"{re.escape(key)}\s*:\s*([\d.]+)"
    match = re.search(pattern, line, re.IGNORECASE)
    return float(match.group(1)) if match else None

def process_file_log(directory, prefix_filter=None):
    """Processes Spark log files in a directory that match a given prefix and extracts selected metrics."""
    records = []
    for filename in os.listdir(directory):
        if not (filename.endswith(".log") and filename.startswith(prefix_filter)):
            continue

        filepath = os.path.join(directory, filename)
        with open(filepath, 'r', encoding='utf-16') as f:
            execution_time = None
            aggregate_resource_allocation = None
            shuffle_write = None
            physical_mem_snapshot = None
            virtual_mem_snapshot = None

            for raw_line in f:
                line = raw_line.strip()
                if execution_time is None and "Execution Time" in line:
                    execution_time = extract_value(line, "Execution Time")
                if aggregate_resource_allocation is None and "Aggregate Resource Allocation" in line:
                    aggregate_resource_allocation = extract_value(line, "Aggregate Resource Allocation")
                if shuffle_write is None and "Shuffle write" in line:
                    shuffle_write = extract_value(line, "Shuffle write")
                if physical_mem_snapshot is None and "Physical Memory Snapshot" in line:
                    physical_mem_snapshot = extract_value(line, "Physical Memory Snapshot")
                if virtual_mem_snapshot is None and "Virtual Memory Snapshot" in line:
                    virtual_mem_snapshot = extract_value(line, "Virtual Memory Snapshot")

            if None not in (
                execution_time,
                physical_mem_snapshot,
                virtual_mem_snapshot,
                shuffle_write,
                aggregate_resource_allocation
            ):
                records.append((
                    execution_time,
                    physical_mem_snapshot,
                    virtual_mem_snapshot,
                    shuffle_write,
                    aggregate_resource_allocation
                ))
    return records

def save_csv(data, output_csv, averages=False):
    """Saves the extracted metrics to a CSV file."""
    header = [
        "execution_time",
        "physical_mem_snapshot",
        "virtual_mem_snapshot",
        "shuffle",
        "aggregate_resource_allocation"
    ]

    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)

        if averages:
            writer.writerow([
                data["execution_time"],
                data["physical_mem_snapshot"],
                data["virtual_mem_snapshot"],
                data["shuffle"],
                data["aggregate_resource_allocation"]
            ])
        else:
            writer.writerows(data)

def calculate_average(data):
    return {
        "execution_time": statistics.mean(row[0] for row in data),
        "physical_mem_snapshot": statistics.mean(row[1] for row in data),
        "virtual_mem_snapshot": statistics.mean(row[2] for row in data),
        "shuffle": statistics.mean(row[3] for row in data),
        "aggregate_resource_allocation": statistics.mean(row[4] for row in data)
    }

def operation_spark(input, output):
    log_folder = input
    output_base = output

    numero_mb = extract_mb_from_path(log_folder)

    for prefix in ["log-spark", "log-rdd-spark"]:
        print(f"Processing files with prefix: {prefix}")
        extracted_data = process_file_log(log_folder, prefix_filter=prefix)

        if not extracted_data:
            print(f"No valid records found for prefix '{prefix}'. Skipping.")
            continue

        averages = calculate_average(extracted_data)

        suffix = prefix[len("log-"):]   # "spark" or "rdd-spark"
        output_csv = os.path.join(output_base, f"log-{numero_mb}MB-{suffix}.csv")

        save_csv(averages, output_csv, averages=True)
        print(f"CSV '{output_csv}' written with averages.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python from-log-to-csv-spark.py <log_folder> <output_base>")
        sys.exit(1)

    log_folder = sys.argv[1]
    output_base = sys.argv[2]  

    operation_spark(log_folder, output_base)
