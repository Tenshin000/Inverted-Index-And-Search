
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re

def csv_to_plot():
    folder_path = "../log/csv-logs"

    data = []
    technologies = {"noimc-hadoop","hadoop","rdd-spark","spark"}
    technology_labels = {
        "spark": "Spark with DataFrames",
        "rdd-spark": "Spark with RDD",
        "hadoop": "Hadoop with In-Mapper Combining",
        "noimc-hadoop": "Hadoop with Combiner"
    }    

    tech_pattern = "|".join(map(re.escape, technologies))

    pattern = re.compile(rf"log-(\d+)(MB)-({tech_pattern})\.csv")

    for filename in os.listdir(folder_path):
        match = pattern.match(filename)
        if match:
            size_mb = int(match.group(1))
            tech = match.group(3)
            
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path, encoding='utf-8', header=0)

            avg_exec = df["execution_time"].mean()
            avg_res = df["aggregate_resource_allocation"].mean()
            avg_shu = df["shuffle"].mean()
            avg_phy = df["physical_mem_snapshot"].mean()
            avg_vir = df["virtual_mem_snapshot"].mean()
            avg_time = df["total_cpu_time"].mean()
            
            data.append({
                "size_mb": size_mb,
                "technology": tech,
                "Execution Time (s)": avg_exec,
                "Aggregate Resource Allocation (MB-s)": avg_res,
                "Shuffle (MB)": avg_shu,
                "Physical Memory Snapshot (MB)": avg_phy,
                "Virtual Memory Snapshot (MB)": avg_vir,
                "Total CPU Time (s)": avg_time
            })

    summary_df = pd.DataFrame(data)
    summary_df["technology"] = summary_df["technology"].map(technology_labels)

    sns.set_theme(style="whitegrid")

    for metric in ["Execution Time (s)", 
                    "Aggregate Resource Allocation (MB-s)", 
                    "Shuffle (MB)",
                    "Physical Memory Snapshot (MB)",
                    "Virtual Memory Snapshot (MB)",
                    "Total CPU Time (s)"]:
        
        plt.figure(figsize=(8, 5))
        
        sns.barplot(
            data=summary_df,
            x="size_mb",
            y=metric,
            hue="technology",
            palette="muted"
        )

        output_dir = "../doc/images/"
        os.makedirs(output_dir, exist_ok=True) 

        title_metric = metric.title().replace("Mb", "MB")
        plt.title(f"{title_metric} vs Input Size (MB)")
        plt.xlabel("Input Size (MB)")
        plt.ylabel(f"Average {title_metric}")
        plt.tight_layout()
        plt.legend(title="Technology")

        filename = os.path.join(output_dir, f"plot_{metric}.png")
        plt.savefig(filename)
        print(f"Saved: {filename}")

        plt.show()
        
        plt.close()

if __name__ == "__main__":
    csv_to_plot()
