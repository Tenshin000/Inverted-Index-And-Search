import os
import re
import csv

# More robust patterns with optional whitespace and units
execution_time_re = re.compile(r"Execution Time                : ([\d.]+)")
total_cpu_time_re = re.compile(r"Total CPU time                : ([\d.]+)")
aggregate_resource_allocation_re = re.compile(r"Aggregate Resource Allocation : ([\d]+)")

# Loop through files with 'spark' in the name
for filename in os.listdir("."):
    if "spark" in filename.lower() and os.path.isfile(filename):
        print(f"Processing file: {filename}")
        with open(filename, "r") as f:
            content = f.read()

        exec_match = execution_time_re.search(content)
        cpu_match = total_cpu_time_re.search(content)
        res_match = aggregate_resource_allocation_re.search(content)

        # Print matches for debugging
        print("Execution Time Match:", exec_match.group(1) if exec_match else "❌ Not found")
        print("Total CPU Time Match:", cpu_match.group(1) if cpu_match else "❌ Not found")
        print("Aggregate Resource Allocation Match:", res_match.group(1) if res_match else "❌ Not found")

        if not all([exec_match, cpu_match, res_match]):
            print(f"❌ Skipping {filename} due to missing values.\n")
            continue

        # Convert values
        data = [
            ["variable", "value"],
            ["execution_time", float(exec_match.group(1))],
            ["total_cpu_time", float(cpu_match.group(1))],
            ["aggregate_resource_allocation", int(res_match.group(1))]
        ]

        # Write to CSV
        csv_filename = filename + ".csv"
        with open(csv_filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(data)

        print(f"✅ Data saved to {csv_filename}\n")
