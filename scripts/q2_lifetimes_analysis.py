# q2_lifetimes_analysis.py
__author__ = "Wayne Decatur" #fomightez on GitHub
__license__ = "MIT"
__version__ = "0.1.0"


# q2_lifetimes_analysis.py by Wayne Decatur
# ver 0.1.0
#
#*******************************************************************************
# 
# PURPOSE: Processes data as advised in Q2 outline
# 
import os
import sys
import glob
import pandas as pd
from halo import HaloNotebook as Halo
import datetime



#*******************************************************************************
##################################
#  DEFINED VALUES        #

##################################
#

## Defines the bins and corresponding labels for breakdown: 
#(based on specifications)
# Stable: >300s
# Long-lived: 300s >= x >90s
# Productive: 90s >= x > 20s
# Abortive <= 20s
# see https://twitter.com/josephofiowa/status/1146043398266195968 about how
# bins are right inclusive
bins_for_breakdown = [-float("inf"),20,90,300,float("inf")]
labels_for_bins=['Abortive', 'Productive', 'Long-lived','Stable']


#
#*******************************************************************************
#***************************END DEFINED VALUES ********************************





################################################################################
#######----------------------HELPER FUNCTIONS-----------------------------######

def extract_sample_id(fn):
    '''
    Takes a file name and extracts the sample id.

    Example input:

    wt pod cmc-lch 100x005_Detailed.csv
    ko pod cmc-lch egfp-m1e-v117p 100x002_Plot.csv
    ko pod cmc-lch egfp-m1e-wt 100x008_Plot.csv

    Example output:
    wt pod
    v117p
    wt
    '''
    if fn.count("-") > 2:
        return fn.lower().split("-")[3].split()[0].strip()
    parts = fn.lower().split()
    return " ".join(parts[:2])

#######------------------END OF HELPER FUNCTIONS--------------------------######
################################################################################









################################################################################
#######------------------------MAIN SECTION-------------------------------######

spinner = Halo(text='Processing...', spinner='dots',color = 'magenta')
spinner.start()


# MAKE A LIST OF THE OPTIONAL ZIP FILES TO UNPACK:
#------------------------------------------------------------------------------#
sys.stderr.write("Checking if any presumably"
    " CVS file-containing uploaded Zip files present...")
zip_files = glob.glob("*.zip")
if zip_files:
    init_csv_files = glob.glob("**/*.csv", recursive=True)
    sys.stderr.write(f"{len(zip_files)} detected.\n")
    sys.stderr.write(f"Unpacking {len(zip_files)} Zip file(s).\n")
    for zip_file in zip_files:
        cmd = f'unzip "{zip_file}"'
        os.system(cmd)
    post_unzip_csv_files = glob.glob("**/*.csv", recursive=True)
    if (len(init_csv_files) < len(post_unzip_csv_files)):
        if init_csv_files:
            sys.stderr.write(f"{len(init_csv_files)} CSV file(s) were directly "
                "uploaded to the main directory.\n")
        sys.stderr.write(f"The {len(zip_files)} uploaded Zip file(s) contain"
            f" {len(post_unzip_csv_files)-len(init_csv_files)} CSV file(s).\n")
        subdirectories_w_csvs = list(
            set([os.path.dirname(pn) for pn in post_unzip_csv_files]))
        # I was seeing some empty directory names, ie. `''` generated. This next
        # line removes the empty ones so it doesn't later count thos in main 
        # directory
        subdirectories_w_csvs = [x for x in subdirectories_w_csvs if x]
        if subdirectories_w_csvs:
            for subdir in subdirectories_w_csvs:
                count_of_subdir_in_paths = sum(
                    subdir in s[:len(subdir)] for s in post_unzip_csv_files)# 
                # based on https://stackoverflow.com/a/45738852/8508004 and I 
                # added the slice on the length of the string to restrict to 
                # start of the path
                sys.stderr.write(f"Subdirectory '{subdir}' contains "
                    f"{count_of_subdir_in_paths} CSV file(s).\n")
    else:
        sys.stderr.write(f"\n\nThe {len(zip_files)} uploaded Zip file(s) "
            "contained NO CSV files. This seems odd! All okay?\n")
else:
    sys.stderr.write("No Zip files detected.\n")


# MAKE A LIST OF THE CSV FILES TO ANALYZE:
#------------------------------------------------------------------------------#
# recursive search for CSV files in current directory or sub directories
csv_files = glob.glob("**/*.csv", recursive=True)
if csv_files:
    sys.stderr.write(f"\n\nProcessing {len(csv_files)} CSV file(s)...\n")
else:
    sys.stderr.write("\n\nNo CSV files detected. That seems odd.\n**"
        "Exiting processing script as there is nothing to do.**\n")
    sys.exit(1)


# GO THROUGH CSV FILES COLLECTING THE DATA:
#------------------------------------------------------------------------------#
collected_durations_dict = {} #initialize a dictionary for collecting the data 
# as it is processed

for fn in csv_files:
    # make a dataframe from the CSV file 
    csv_df = pd.read_csv(fn, header=2)
    #collect the track dureation column
    duration_track = csv_df["Track Duration"].tolist()
    #store the list with the filename, minus the .csv extension, as key
    collected_durations_dict[fn[:-4]] = duration_track

# COMBINE COLLECTED DATA BASED ON SAMPLE ID:
#------------------------------------------------------------------------------#
from collections import defaultdict
collected_durations_dict_by_sample = defaultdict(list)
for fn,track in collected_durations_dict.items():
    collected_durations_dict_by_sample[extract_sample_id(fn).upper()] += track

# SORT THE COMBINED DATA:
#------------------------------------------------------------------------------#
[collected_durations_dict_by_sample[s].sort(
    ) for s in collected_durations_dict_by_sample]


# MAKE A DATAFRAME FROM THE COMBINED DATA:
#------------------------------------------------------------------------------#
#df = pd.DataFrame(collected_durations_dict) # cannot do this directly like that 
# because the lists can be different length and get :
# 'ValueError: arrays must all be same length' Fix below based on 
# https://stackoverflow.com/a/40442094/8508004 , which makes it from a dict with
# uneven lists
df = pd.DataFrame.from_dict(collected_durations_dict_by_sample, orient='index')
df = df.transpose()

# SAVE THE DATAFRAME AS CSV and EXCEL:
#------------------------------------------------------------------------------#
now = datetime.datetime.now()
spreadsheet_name_prefix = f"lifetimes_collected{now.strftime('%b%d%Y%H%M')}"
df.to_excel(spreadsheet_name_prefix+'.xlsx')
df.to_csv(spreadsheet_name_prefix+ '.tsv', sep='\t',index = False)


# CATEGORIZE BASED ON LIFETIMES SPECIFICATIONS:
#------------------------------------------------------------------------------#    
#df['lifetime category'] = pd.cut(
#    df.WT, bins=bins_for_breakdown, labels=labels_for_bins) # based on 
# https://stackoverflow.com/a/32801170/8508004  

# CATEGORIZE BASED ON LIFETIMES SPECIFICATIONS & MAKE A BREAKDOWN OF CATEGORIES:
#------------------------------------------------------------------------------#
count_dfs = []
cat_col_name = 'lifetime category'
for sample in collected_durations_dict_by_sample:
    df_for_counts = df.copy()
    df_for_counts[cat_col_name] = pd.cut(
        df[sample], bins=bins_for_breakdown, labels=labels_for_bins)
    df_for_counts = df_for_counts.groupby(
        by=cat_col_name).size().reset_index(name=sample) # based on 
    # https://stackoverflow.com/a/32801170/8508004
    count_dfs.append(df_for_counts)
from functools import reduce
counts_df = reduce(
    lambda left,right: pd.merge(left,right,on=cat_col_name), count_dfs)
# make coloumns multiindex
#multi_tuples = [(
#    f'{cat_col_name}',' '), ('counts','wt'), ('counts','other')] # for 
# development
multi_tuples = [(
    f'{cat_col_name}',' ')] + [('counts',x) for x in counts_df.columns[1:]]
multi_cols = pd.MultiIndex.from_tuples(multi_tuples)
counts_df.columns=multi_cols
counts_file_name_prefix = f"lifetimes_breakdown{now.strftime('%b%d%Y%H%M')}"
counts_df.to_excel(counts_file_name_prefix+'.xlsx')
counts_df.to_csv(counts_file_name_prefix+ '.tsv', sep='\t',index = False)


spinner.stop()




# CLEAN UP SO DIRECTORY NOT SO CLUTTERED SO MORE OBVIOUS WHAT TO DOWNLOAD:
#------------------------------------------------------------------------------#  
# Clean up by:
# 1. deleting the input files (even if added via unpacked zip files) so easy to 
# find the derived data files generated
cleaning_step = True
if cleaning_step:
    sys.stderr.write("\nCleaning up...be patient...\n")
    spinner = Halo(text='Cleaning up...', spinner='dots',color = 'magenta')
    spinner.start()  
    for zip_file in zip_files:
        os.remove(zip_file) # this way it won't get reused on subsequent 
        # processing runs
    for pn in csv_files:
        os.remove(pn)
    if zip_files:
        # There may be directories made from the zip files that correspond to
        # dates for that set of data. To clean up better it will be nice to also
        # remove those. The CSV files will already have been deleted but without 
        # this step the empty directories will be left behind. Only delete the
        # unpacked directories after the deletion of the `csv_files` though so 
        # all those files will exist when `os.remove(pn)` step gets run and so 
        # won't throw errors trying to delete them if already gone.
        list_of_subdirectories = list(
            set([os.path.dirname(pn) for pn in csv_files]))
        # I was seeing some empty directory names, ie. `''` generated. This next
        # line removes the empty ones
        list_of_subdirectories = [x for x in list_of_subdirectories if x]
        import shutil
        [shutil.rmtree(each_dir) for each_dir in list_of_subdirectories]
        # when the archive gets made on a MAC OS and uploaded to Linux, there 
        # can be `__MACOSX` directory that also gets made and this can delete
        # that recursively, evem if not empty to further clean
        dn_of_macosx_artifact_directory = "__MACOSX"
        if os.path.isdir(dn_of_macosx_artifact_directory):
            shutil.rmtree(dn_of_macosx_artifact_directory)
    #spinner.stop()
    sys.stderr.write("\nCleaning complete.")
    sys.stderr.write("\nAll data uploaded to this remote system has been "
        "deleted\nto make the resulting files from the processing easier to "
        "locate.")
#######------------------END OF MAIN SECTION------------------------------######
################################################################################