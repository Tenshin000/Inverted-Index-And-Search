import argparse
import getpass

from recover_resources import download_txt_files

def main():
    parser = argparse.ArgumentParser(description="Inverse Index Search CLI")
    parser.add_argument("--recover-resources", action="store_true", help="Recover the files from Altervista and put them in HDFS")
    parser.add_argument("--recover-resources-local", action="store_true", help="Recover the files from Altervista and put them in local")

    args = parser.parse_args()

    flags = [
        args.recover_resources,
        args.recover_resources_local
    ]
    total_steps = sum(1 for f in flags if f)
    step = 1

    if args.recover_resources:
        print(f"[{step}/{total_steps}] ➡ Resource recovery activated ...")
        ftp_pass = getpass.getpass("Please. Enter your FTP password: ")
        download_txt_files(
            base_url="ftp.blogpanattoni.altervista.org" ,
            folder_name="resources",
            output_dir="hdfs:///user/hadoop/inverted-index/data",
            ftp_user="blogpanattoni",
            ftp_pass=ftp_pass
        )
        download_txt_files()
        print(f"[{step}/{total_steps}] ✓ Resources obtained.")
        step += 1

    if args.recover_resources_local:
        print(f"[{step}/{total_steps}] ➡ Resource recovery activated ...")
        ftp_pass = getpass.getpass("Please. Enter your FTP password: ")
        download_txt_files(
            base_url="ftp.blogpanattoni.altervista.org" ,
            folder_name="resources",
            output_dir="data",
            ftp_user="blogpanattoni",
            ftp_pass=ftp_pass
        )
        download_txt_files()
        print(f"[{step}/{total_steps}] ✓ Resources obtained.")
        step += 1

    

if __name__ == "__main__":
    main()
