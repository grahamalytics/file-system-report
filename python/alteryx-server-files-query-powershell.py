import datetime as dt
import logging
import pandas as pd
import subprocess
import sys

# Environment Setup #
#####################

# capture today's date in order to calculate age of file, as well as month and year
today = dt.datetime.today()

today_mmddyyyy = today.strftime("%m-%d-%Y")

month_year = today.strftime("%B %Y").split()

# configure logging for this script
logging.basicConfig(filename="F:\\Alteryx\\server-files-report\\python\\logs\\server-files-app-log-{0}.txt".format(today_mmddyyyy),
                    level=logging.INFO,
                    format="%(asctime)s %(levelname)s:%(message)s")

# instantiate empty dictionary object to store information on server files
results = {}

# instantiate incrementer for looping through and processing results
i = 0


# Call PowerShell Script #
##########################

logging.info("Remotely executing PowerShell script via Python subprocess.")
logging.info("The script will walk the server's filesystem and return information on each file, its size, age, and owner")

try:
    # call PowerShell and execute script returning info on every file on server
    ps_pipe = subprocess.Popen(["powershell.exe",
                                r"F:\Alteryx\server-files-report\powershell\02BackUp-fs-stats.ps1"],
                               stdout=subprocess.PIPE)
except Exception as e:
    logging.error("Failed to successfully execute PowerShell script: {0}".format(str(e)))
    sys.exit(1)

logging.info("Successfully completed running PowerShell script.")

# Results Processing #
######################

logging.info("Parsing results of PowerShell script.")


    # loop through response (in bytes) and decode it before parsing out key:value pairs of information
    # and storing in "results" dictionary
for bytes_row in ps_pipe.stdout:
    row = bytes_row.decode("ISO-8859-15")
    try:
        if ": " in row:
            key, value = [x.strip() for x in row.split(": ")]

            if key == "LastWriteTime":
                results[i] = {}
                results[i][key] = value
            elif key != "Owner":
                results[i][key] = value
            else:
                results[i][key] = value
                i += 1
        else:
            continue
    except Exception as e:
        logging.error("Failed to parse results from PowerShell script: {0} {1}".format(str(e), str(row)))

logging.info("Completed parsing results from PowerShell script.")
logging.warning("These may be partial results, check logs to see if any records failed to parse.")

# Results Formatting #
######################

logging.info("Beginning to format PowerShell results into tabular format.")

try:
    # create panda's dataframe from our results dictionary
    results_df = pd.DataFrame.from_dict(results, orient="index")

    # format the LAST_WRITE_DATETIME field to be a datetime object
    results_df["LAST_WRITE_DATETIME"] = pd.to_datetime(results_df["LastWriteTime"])

    # calculate age of file in a new field FILE_AGE_TIMEDELTA
    results_df["FILE_AGE_TIMEDELTA"] = today - results_df["LAST_WRITE_DATETIME"]

    # extract days from FILE_AGE_TIMEDELTA and save in new variable FILE_AGE_DAYS
    results_df["FILE_AGE_DAYS"] = results_df["FILE_AGE_TIMEDELTA"].astype("timedelta64[D]")

    # use regex to remove commmon prefix from OWNER field and save into new variable called OWNER_FRMT
    results_df["OWNER_FRMT"] = results_df["Owner"].str.replace(r"BRIERLEY\\", "", regex=True)

    # use string extraction to create DIR_PATH field
    results_df["DIR_PATH"] = results_df["FullName"].str.extract("(^F:\\\.+\\\)", expand=False)

    # concatenate DIR_PATH and NAME to create FULL_PATH field
    results_df["FULL_PATH"] = results_df['DIR_PATH'] + results_df["Name"]

    # convert BYTES_B to numeric
    results_df["BYTES_B"] = pd.to_numeric(results_df["Length"])

    # convert bytes to kilobytes
    results_df["KILOBYTES_KB"] = results_df["BYTES_B"] / 1024.0

    # convert kilobytes to megabytes
    results_df["MEGABYTES_MB"] = results_df["KILOBYTES_KB"] / 1024.0

    # convert megabytes to gigabytes
    results_df["GIGABYTES_GB"] = results_df["MEGABYTES_MB"] / 1024.0
except Exception as e:
    logging.error("Failed to format PowerShell results into tabular format: {0}".format(str(e)))
    sys.exit(1)

logging.info("Successfully formated PowerShell results into tabular format.")

# Results Output #
##################

logging.info("Writing formatted results to server.")

try:
    # write results to disk after dropping columns, renaming headers, and reindexing the dataframe
    results_df.drop(columns=["LastWriteTime", "Length", "Owner", "FILE_AGE_TIMEDELTA"])\
        .rename(index=str, columns={"": "INDEX", "OWNER_FRMT": "OWNER", "Name": "FILE_NAME"})\
        .reindex(columns=["OWNER", "FILE_NAME", "FILE_AGE_DAYS", "BYTES_B", "KILOBYTES_KB", "MEGABYTES_MB", "GIGABYTES_GB", "LAST_WRITE_DATETIME", "DIR_PATH", "FULL_PATH"])\
        .to_csv("F:\\Alteryx\\server-files-report\\alteryx\\input\\alteryx-server-files-results-{0}{1}.csv".format(month_year[0], month_year[1]), index=True, index_label="INDEX")
except Exception as e:
    logging.error("Problems while trying to write results to server: {0}".format(str(e)))
    sys.exit(1)

logging.info("Successfully finished application, exiting normally.")
sys.exit(0)