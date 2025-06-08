import subprocess
import shlex
import sys

BASE_PATH = "hdfs:///user/hadoop/inverted-index"
SRC_DIR = f"{BASE_PATH}/data"
THRESHOLDS_MB = [128, 256, 512, 1024]  # in MB

def run(cmd):
    """
    Execute a shell command and return its stdout.
    Exits the program if the command fails.
    """
    print(f"   ↪ Running: {cmd}")  
    proc = subprocess.run(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        print(f"ERROR running `{cmd}`:\n{proc.stderr}", file=sys.stderr)
        sys.exit(1)
    return proc.stdout

def list_txt_files(src_dir):
    """
    Return a list of tuples (path, size_bytes) for each .txt file in src_dir,
    sorted alphabetically by path.
    """
    cmd = f"hdfs dfs -ls {src_dir}/*.txt"
    out = run(cmd).strip().splitlines()
    files = []
    for line in out:
        parts = line.split()
        size = int(parts[4])
        path = parts[-1]
        files.append((path, size))
    return sorted(files, key=lambda x: x[0])

def make_dest_dir(th_mb):
    """
    Create the destination directory in HDFS for a given threshold.
    """
    dest = f"{BASE_PATH}/data-{th_mb}MB"
    run(f"hdfs dfs -mkdir -p {dest}")
    return dest

def copy_files(file_list, dest_dir, threshold_bytes):
    """
    Copy files from file_list into dest_dir until the total copied size
    reaches or exceeds threshold_bytes.
    """
    total = 0
    count = 0
    for path, size in file_list:
        if total >= threshold_bytes:
            break
        print(f"   → Copying: {path} ({size/1024/1024:.2f} MB)")
        run(f"hdfs dfs -cp {path} {dest_dir}/")
        total += size
    print(f"  Copied {count} files, totaling ~{total/1024/1024:.2f} MB to {dest_dir}")

def create_partitions():
    print("1) Collecting list of .txt files from HDFS...")
    files = list_txt_files(SRC_DIR)
    total_mb = sum(s for _, s in files) / 1024 / 1024
    print(f"   → Found {len(files)} .txt files, total size ~{total_mb:.2f} MB")

    i = 2
    for mb in THRESHOLDS_MB:
        print(f"\n{i}) Creating a {mb} MB partition:")
        dest = make_dest_dir(mb)
        print(f"   → Destination directory: {dest}")
        copy_files(files, dest, mb * 1024 * 1024)
        i += 1

def main():
    create_partitions()

if __name__ == "__main__":
    main()
