import csv
import os
import re
import statistics
import sys

def extract_execution_time(line):
    """Extracts the numeric execution time (in seconds) from a line."""
    match = re.search(r"Execution Time\s*[:=]\s*([\d\.]+)", line, re.IGNORECASE)
    return float(match.group(1)) if match else None


def process_non_parallel_logs(directory):
    """
    Processes all log-non-parallel<N>.log files in the given directory,
    extracting the execution time from each.
    Returns a list of (index, execution_time) tuples.
    """
    data = []
    pattern = re.compile(r"log-non-parallel(\d+)\.log$")

    for filename in os.listdir(directory):
        m = pattern.match(filename)
        if not m:
            continue
        idx = int(m.group(1))
        filepath = os.path.join(directory, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            time_val = None
            for raw_line in f:
                if "Execution Time" in raw_line:
                    time_val = extract_execution_time(raw_line)
                    break
        if time_val is not None:
            data.append((idx, time_val))
        else:
            print(f"Warning: no execution time found in {filename}")

    # Sort by index
    data.sort(key=lambda x: x[0])
    return data


def calculate_average_times(data):
    """
    Given a list of execution times, compute the average.
    """
    times = [t for _, t in data]
    return statistics.mean(times) if times else None


def save_csv(data, output_csv, averages=False):
    """
    Writes either all execution times or only the average to CSV.
    If averages=True, data is expected to be a single float.
    Otherwise, data is a list of (index, time) tuples.
    """
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["index", "execution_time"] if not averages else ["execution_time"] )

        if averages:
            writer.writerow([data])
        else:
            for idx, time_val in data:
                writer.writerow([idx, time_val])


def operation_non_parallel(log_folder, output_csv, write_average=False):
    """
    Main entry point:
      - log_folder: directory containing log-non-parallel<N>.log files
      - output_csv: path to output CSV file
      - write_average: if True, writes only the average execution time
    """
    records = process_non_parallel_logs(log_folder)
    if not records:
        print("No valid log-non-parallel<N>.log files found or no execution times extracted.")
        return

    if write_average:
        avg = calculate_average_times(records)
        save_csv(avg, output_csv, averages=True)
        print(f"Wrote average execution time to {output_csv}")
    else:
        save_csv(records, output_csv, averages=False)
        print(f"Wrote execution times for {len(records)} files to {output_csv}")

def main(log_folder, output_csv, write_average=False):
    operation_non_parallel(log_folder, output_csv, write_average)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract Execution Time from log-non-parallel<N>.log files and save to CSV."
    )
    parser.add_argument("log_folder", help="Folder containing the log files.")
    parser.add_argument("output_csv", help="Output CSV file path.")
    parser.add_argument(
        "--average", action="store_true",
        help="Write only the average execution time instead of all records."
    )
    args = parser.parse_args()

    main(args.log_folder, args.output_csv, write_average=args.average)
