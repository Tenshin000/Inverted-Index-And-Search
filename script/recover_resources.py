import os
import ftplib
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
import getpass
import subprocess
import tempfile
import shutil

# Configuration
BASE_URL = "ftp.blogpanattoni.altervista.org"
FOLDER_NAME = "resources"
OUTPUT_DIR = "hdfs:///user/hadoop/inverted-index/data"  # or "data" for local test
FTP_USER = "blogpanattoni"


def is_hdfs_path(path):
    """Check if the given path is an HDFS path."""
    return path.startswith("hdfs://")


def make_hdfs_dir(hdfs_dir):
    """Create a directory in HDFS using hdfs dfs -mkdir -p."""
    try:
        subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_dir], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error creating HDFS directory: {e}")


def upload_to_hdfs(local_file, hdfs_dir):
    """Upload a local file to the specified HDFS directory."""
    try:
        subprocess.run(['hdfs', 'dfs', '-put', '-f', local_file, hdfs_dir], check=True)
        print(f"Uploaded {local_file} -> {hdfs_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading to HDFS: {e}")


def download_txt_files(base_url, folder_name, output_dir, ftp_user=None, ftp_pass=None):
    """
    Download all .txt files via HTTP(S) or FTP into a local or HDFS folder.

    :param base_url: HTTP(S) site URL or FTP host.
    :param folder_name: Remote directory containing .txt files.
    :param output_dir: Destination directory (local or HDFS).
    :param ftp_user: FTP username (defaults to anonymous if None).
    :param ftp_pass: FTP password.
    """
    parsed = urlparse(base_url)
    scheme = parsed.scheme.lower()

    use_hdfs = is_hdfs_path(output_dir)
    if use_hdfs:
        make_hdfs_dir(output_dir)
        temp_dir = tempfile.mkdtemp()
        print(f"Temporary download directory: {temp_dir}")
    else:
        os.makedirs(output_dir, exist_ok=True)
        temp_dir = output_dir

    try:
        # HTTP/HTTPS case
        if scheme in ("http", "https"):
            url = f"{base_url.rstrip('/')}/{folder_name}/"
            response = requests.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            txt_files = [a['href'] for a in soup.find_all('a', href=True) if a['href'].lower().endswith('.txt')]

            for fname in txt_files:
                file_url = f"{url}{fname}"
                local_path = os.path.join(temp_dir, fname)
                print(f"Downloading {file_url} -> {local_path}")
                file_resp = requests.get(file_url)
                file_resp.raise_for_status()
                with open(local_path, 'wb') as f:
                    f.write(file_resp.content)

                if use_hdfs:
                    upload_to_hdfs(local_path, output_dir)

        # FTP case
        else:
            host = parsed.netloc or base_url
            ftp = ftplib.FTP()
            ftp.encoding = 'latin-1'
            ftp.connect(host)
            ftp.login(ftp_user or 'anonymous', ftp_pass or '')
            ftp.cwd(folder_name)

            for fname in ftp.nlst():
                if fname.lower().endswith('.txt'):
                    local_path = os.path.join(temp_dir, fname)
                    print(f"Downloading ftp://{host}/{folder_name}/{fname} -> {local_path}")
                    with open(local_path, 'wb') as f:
                        ftp.retrbinary(f"RETR {fname}", f.write)

                    if use_hdfs:
                        upload_to_hdfs(local_path, output_dir)
            ftp.quit()
    finally:
        if use_hdfs:
            shutil.rmtree(temp_dir)
            print(f"Temporary directory {temp_dir} cleaned up.")


if __name__ == "__main__":
    FTP_PASS = getpass.getpass('Enter your FTP password: ')
    download_txt_files(BASE_URL, FOLDER_NAME, OUTPUT_DIR, FTP_USER, FTP_PASS)
