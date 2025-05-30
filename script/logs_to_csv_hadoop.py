import csv
import os
import re
import statistics
import sys

def extract_value(line, key):
    """Extracts a numeric value from a line based on the given key."""
    pattern = rf"{re.escape(key)}\s*[:=]\s*([\d.]+)"
    match = re.search(pattern, line, re.IGNORECASE)
    return float(match.group(1)) if match else None

def process_file_log(directory, prefix_filter):
    """Processes Hadoop log files with a specific prefix and extracts metrics."""
    data = []
    MB = 1024 * 1024

    for filename in os.listdir(directory):
        if not (filename.startswith(prefix_filter) and filename.endswith(".log")):
            continue

        filepath = os.path.join(directory, filename)
        with open(filepath, 'r', encoding='utf-16') as f:
            execution_time = total_cpu_time = aggregate_resource_allocation = None
            reduce_shuffle = physical_mem_snapshot = virtual_mem_snapshot = None

            for raw_line in f:
                line = raw_line.strip()
                if execution_time is None and "Execution Time" in line:
                    execution_time = extract_value(line, "Execution Time")
                if total_cpu_time is None and "CPU time spent" in line:
                    total_cpu_time = extract_value(line, "CPU time spent (ms)")
                if aggregate_resource_allocation is None and "Aggregate Resource Allocation" in line:
                    aggregate_resource_allocation = extract_value(line, "Aggregate Resource Allocation")
                if reduce_shuffle is None and "Reduce shuffle bytes" in line:
                    reduce_shuffle = extract_value(line, "Reduce shuffle bytes") / MB
                if physical_mem_snapshot is None and "Physical memory (bytes) snapshot" in line:
                    physical_mem_snapshot = extract_value(line, "Physical memory (bytes) snapshot") / MB
                if virtual_mem_snapshot is None and "Virtual memory (bytes) snapshot" in line:
                    virtual_mem_snapshot = extract_value(line, "Virtual memory (bytes) snapshot") / MB

            if None not in (
                execution_time,
                physical_mem_snapshot,
                virtual_mem_snapshot,
                total_cpu_time,
                reduce_shuffle,
                aggregate_resource_allocation
            ):
                data.append((
                    execution_time,
                    physical_mem_snapshot,
                    virtual_mem_snapshot,
                    total_cpu_time,
                    reduce_shuffle,
                    aggregate_resource_allocation
                ))

    return data

def save_csv(data, output_csv, averages=False):
    header = [
        "execution_time", 
        "physical_mem_snapshot",
        "virtual_mem_snapshot",
        "total_cpu_time", 
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
                data["total_cpu_time"],
                data["shuffle"],
                data["aggregate_resource_allocation"]
            ])
        else:
            writer.writerows(data)

def calculate_average(data):
    average = {
        "execution_time": statistics.mean(row[0] for row in data),
        "physical_mem_snapshot": statistics.mean(row[1] for row in data),
        "virtual_mem_snapshot": statistics.mean(row[2] for row in data),
        "total_cpu_time": statistics.mean(row[3] for row in data),
        "shuffle": statistics.mean(row[4] for row in data),
        "aggregate_resource_allocation": statistics.mean(row[5] for row in data)
    }
    return average

def operation_hadoop(input, output):
    log_folder = input
    output_base = output

    for prefix in ["log-hadoop", "log-noimc-hadoop"]:
        print(f"Processing files with prefix: {prefix}")
        extracted_data = process_file_log(log_folder, prefix_filter=prefix)

        if not extracted_data:
            print(f"No valid records found for prefix '{prefix}'. Skipping.")
            continue

        averages = calculate_average(extracted_data)

        output_csv = os.path.join(output_base, f"{prefix}.csv")
        save_csv(averages, output_csv, averages=True)
        print(f"CSV '{output_csv}' written with averages.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python from-log-to-csv-hadoop.py <log_folder> <output_base>")
        sys.exit(1)

    log_folder = sys.argv[1]
    output_base = sys.argv[2] 

    operation_hadoop(log_folder, output_base)