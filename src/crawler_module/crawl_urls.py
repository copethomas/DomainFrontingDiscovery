import os
import sys
sys.path.append("..")
import concurrent.futures
import subprocess
import pandas as pd
import time
from datetime import timedelta
from Utils import FrontingUtils

config = FrontingUtils.get_config()

# Assign the input file name to a variable
input_file = config['FILE_PATHS']['cdn_domain_mapping_file_path']

# Check if the input file exists
if not os.path.isfile(input_file):
    print(f"Input file '{input_file}' not found.")
    sys.exit(1)

for option in config['DIR_PATHS']:
    dir_path = config['DIR_PATHS'][option]
    os.makedirs(dir_path, exist_ok=True)

# Global variables for status tracking
total_domains = 0
completed_domains = 0
successful_domains = 0
failed_domains = 0
start_time = 0

def crawl_domain(row):
    global completed_domains, successful_domains, failed_domains

    try:
        cdn = row["cdn"]
        domain = row["domain_sld"]
        print(f"Crawling {domain} from CDN :: {cdn}")
        os.makedirs(f"{config['DIR_PATHS']['crawling_results_path']}{cdn}_{domain}", exist_ok=True)

        # Run the subprocess and capture the result
        result = subprocess.run(
            ["node", "crawler.js", domain, cdn, config['DIR_PATHS']['crawling_results_path']], 
            capture_output=True,
            text=True
        )

        # Check the return code
        if result.returncode != 0:
            print(f"Error while crawling {domain}: Process exited with code {result.returncode}")
            print(f"Error output: {result.stderr}")
            failed_domains += 1
            success = False
        else:
            successful_domains += 1
            success = True

        # Update completed count
        completed_domains += 1

        # Print progress every 10 domains
        if completed_domains % 10 == 0 or completed_domains == total_domains:
            elapsed_time = time.time() - start_time
            domains_per_second = completed_domains / elapsed_time if elapsed_time > 0 else 0
            remaining_domains = total_domains - completed_domains

            # Calculate ETA
            eta_seconds = remaining_domains / domains_per_second if domains_per_second > 0 else 0
            eta = str(timedelta(seconds=int(eta_seconds)))

            print(f"\n--- Progress Update ---")
            print(f"Completed: {completed_domains}/{total_domains} domains ({completed_domains/total_domains*100:.1f}%)")
            print(f"Successful: {successful_domains}, Failed: {failed_domains}")
            print(f"Remaining: {remaining_domains} domains")
            print(f"Elapsed time: {str(timedelta(seconds=int(elapsed_time)))}")
            print(f"Estimated time remaining: {eta}")
            print(f"------------------------\n")

        return success
    except Exception as e:
        print(f"Exception occurred while processing {domain}: {str(e)}")
        failed_domains += 1
        completed_domains += 1

        # Print progress in case of exception too
        if completed_domains % 10 == 0 or completed_domains == total_domains:
            elapsed_time = time.time() - start_time
            domains_per_second = completed_domains / elapsed_time if elapsed_time > 0 else 0
            remaining_domains = total_domains - completed_domains

            # Calculate ETA
            eta_seconds = remaining_domains / domains_per_second if domains_per_second > 0 else 0
            eta = str(timedelta(seconds=int(eta_seconds)))

            print(f"\n--- Progress Update ---")
            print(f"Completed: {completed_domains}/{total_domains} domains ({completed_domains/total_domains*100:.1f}%)")
            print(f"Successful: {successful_domains}, Failed: {failed_domains}")
            print(f"Remaining: {remaining_domains} domains")
            print(f"Elapsed time: {str(timedelta(seconds=int(elapsed_time)))}")
            print(f"Estimated time remaining: {eta}")
            print(f"------------------------\n")

        return False

df_cdn_domains = pd.read_csv(input_file, header = 0)
df_cdn_domains = df_cdn_domains[['cdn','domain_sld']].drop_duplicates()

# Initialize global variables for status tracking
total_domains = len(df_cdn_domains)
completed_domains = 0
successful_domains = 0
failed_domains = 0
start_time = time.time()

print(f"Starting to crawl {total_domains} domains...")

# Create a ThreadPoolExecutor with 30 worker threads
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    # Submit all tasks to the executor
    futures = [executor.submit(crawl_domain, row) for _, row in df_cdn_domains.iterrows()]

    # Wait for all tasks to complete
    concurrent.futures.wait(futures)

#We will sort this later
#FrontingUtils.filter_urls(config['FILE_PATHS']['cdn_domain_mapping_file_path'], config['FILE_PATHS']['domain_url_mapping_file_path'])
