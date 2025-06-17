import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re


def plot_logs():
    folder_path = "../log/csv-logs"

    technologies = {"noimc-hadoop", "hadoop", "rdd-spark", "spark", "non-parallel"}
    technology_labels = {
        "spark": "Spark with DataFrames",
        "rdd-spark": "Spark with RDDs",
        "hadoop": "Hadoop with In-Mapper Combining",
        "noimc-hadoop": "Hadoop with Combiner",
        "non-parallel": "Non-Parallel Execution"
    }

    tech_pattern = "|".join(map(re.escape, technologies))
    pattern = re.compile(rf"log-(\d+)(MB)-({tech_pattern})\.csv")

    data = []
    for filename in os.listdir(folder_path):
        match = pattern.match(filename)
        if not match:
            continue
        size_mb = int(match.group(1))
        tech = match.group(3)
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path, encoding='utf-8')

        entry = {
            "size_mb": size_mb,
            "technology": technology_labels.get(tech, tech),
            "Execution Time (s)": df["execution_time"].mean()
        }
        if tech != "non-parallel":
            entry.update({
                "Aggregate Resource Allocation (MB-s)": df["aggregate_resource_allocation"].mean(),
                "Shuffle (MB)": df["shuffle"].mean(),
                "Physical Memory Snapshot (MB)": df["physical_mem_snapshot"].mean(),
                "Virtual Memory Snapshot (MB)": df["virtual_mem_snapshot"].mean(),
                "Total CPU Time (s)": df["total_cpu_time"].mean()
            })
        data.append(entry)

    summary_df = pd.DataFrame(data)

    sns.set_theme(style="whitegrid")
    output_dir = "../doc/images/"
    os.makedirs(output_dir, exist_ok=True)

    # prendi tutti e 9 i colori di Set1
    deep = sns.color_palette("deep", 4)

    # ricava la palette esplicita
    palette = {
        # rosso scuro (indice 0) per Spark with DataFrames
        "Spark with DataFrames": deep[3],
        # blu (indice 1) per Hadoop with In-Mapper Combining
        "Hadoop with In-Mapper Combining": deep[0],
        # verde scuro (indice 2) per Spark with RDDs
        "Spark with RDDs": deep[2],
        # giallo scuro (indice 5) per Hadoop with Combiner
        "Hadoop with Combiner": deep[1],
        # nero per Non-Parallel Execution
        "Non-Parallel Execution": (0.0, 0.0, 0.0)
    }

    # Plot Execution Time with all technologies
    plt.figure(figsize=(8, 5))
    sns.barplot(
        data=summary_df,
        x="size_mb",
        y="Execution Time (s)",
        hue="technology",
        palette=palette
    )
    plt.title("Execution Time (s) vs Input Size (MB)")
    plt.xlabel("Input Size (MB)")
    plt.ylabel("Average Execution Time (s)")
    plt.tight_layout()
    plt.legend(title="Technology")
    filename = os.path.join(output_dir, "plot_execution_time.png")
    plt.savefig(filename)
    print(f"Saved: {filename}")
    plt.show()
    plt.close()

    # Plot other metrics excluding non-parallel
    other_metrics = [
        "Aggregate Resource Allocation (MB-s)",
        "Shuffle (MB)",
        "Physical Memory Snapshot (MB)",
        "Virtual Memory Snapshot (MB)",
        "Total CPU Time (s)"
    ]
    df_other = summary_df.dropna(subset=other_metrics)
    for metric in other_metrics:
        plt.figure(figsize=(8, 5))
        sns.barplot(
            data=df_other,
            x="size_mb",
            y=metric,
            hue="technology"
        )
        plt.title(f"{metric} vs Input Size (MB)")
        plt.xlabel("Input Size (MB)")
        plt.ylabel(f"Average {metric}")
        plt.tight_layout()
        plt.legend(title="Technology")
        filename = os.path.join(output_dir, f"plot_{metric.replace(' ', '_').lower()}.png")
        plt.savefig(filename)
        print(f"Saved: {filename}")
        plt.show()
        plt.close()


def plot_reducers():
    folder_path = "../log/csv-logs"

    # Match files: log-<DIM>MB-reducer<NUM>.csv
    reducer_pattern = re.compile(r"log-(\d+)(MB)-reducer(\d+)\.csv")
    reducer_data = []

    for filename in os.listdir(folder_path):
        match = reducer_pattern.match(filename)
        if match:
            size_mb = int(match.group(1))
            reducer_num = int(match.group(3))

            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path, encoding='utf-8', header=0)

            reducer_data.append({
                "Input Size (MB)": size_mb,
                "Reducers": reducer_num,
                "Execution Time (s)": df["execution_time"].mean(),
                "Aggregate Resource Allocation (MB-s)": df["aggregate_resource_allocation"].mean(),
                "Shuffle (MB)": df["shuffle"].mean(),
                "Physical Memory Snapshot (MB)": df["physical_mem_snapshot"].mean(),
                "Virtual Memory Snapshot (MB)": df["virtual_mem_snapshot"].mean(),
                "Total CPU Time (s)": df["total_cpu_time"].mean()
            })

    if reducer_data:
        reducer_df = pd.DataFrame(reducer_data)

        # Ensure 'Reducers' is treated as category to maintain order
        reducer_df["Reducers"] = reducer_df["Reducers"].astype(str)

        sns.set_theme(style="whitegrid")
        color_palette = sns.color_palette("Set3", n_colors=reducer_df["Reducers"].nunique())

        for metric in [
            "Execution Time (s)",
            "Aggregate Resource Allocation (MB-s)",
            "Shuffle (MB)",
            "Physical Memory Snapshot (MB)",
            "Virtual Memory Snapshot (MB)",
            "Total CPU Time (s)"
        ]:
            plt.figure(figsize=(8, 5))

            sns.barplot(
                data=reducer_df,
                x="Input Size (MB)",
                y=metric,
                hue="Reducers",
                palette=color_palette
            )

            plt.title(f"{metric} vs Input Size (MB) by Reducer Count")
            plt.xlabel("Input Size (MB)")
            plt.ylabel(f"Average {metric}")
            plt.tight_layout()
            plt.legend(title="Reducers")

            output_dir = "../doc/images/"
            os.makedirs(output_dir, exist_ok=True)
            filename = os.path.join(output_dir, f"plot_reducers_{metric}.png")
            plt.savefig(filename)
            print(f"Saved: {filename}")
            plt.show()
            plt.close()


def csv_to_plot():
    plot_logs()
    plot_reducers()


if __name__ == "__main__":
    csv_to_plot()
