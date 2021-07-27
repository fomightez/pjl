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
import fnmatch
import datetime






################################################################################
#######----------------------HELPER FUNCTIONS-----------------------------######

def extract_sample_id(fn):
    '''
    Takes a file name and extracts the sample id.

    Example input:

    ko pod cmc-lch egfp-m1e-v117p 100x002_Plot.csv
    ko pod cmc-lch egfp-m1e-wt 100x008_Plot.csv

    Example output:
    v117p
    wt
    '''
    return fn.lower().split("-")[3].split()[0].strip()

#######------------------END OF HELPER FUNCTIONS--------------------------######
################################################################################









################################################################################
#######------------------------MAIN SECTION-------------------------------######

spinner = Halo(text='Processing...', spinner='dots',color = 'magenta')
spinner.start()


# MAKE A LIST OF THE CSV FILES TO ANALYZE:
#------------------------------------------------------------------------------#
csv_files = glob.glob("*.csv")

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
collected_durations_dict = {k: v.sorted(
    ) for k,v collected_durations_dict.items()}


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
    


spinner.stop()




# CLEAN UP SO DIRECTORY NOT SO CLUTTERED SO MORE OBVIOUS WHAT TO DOWNLOAD:
#------------------------------------------------------------------------------#  
# Clean up by:
# 1. deleting the input files so easy to find the derived data files generated
cleaning_step = True
if cleaning_step:
    sys.stderr.write("\nCleaning up...be patient...\n")
    spinner = Halo(text='Cleaning up...', spinner='dots',color = 'magenta')
    spinner.start()  
    for fn in csv_files:
        os.remove(fn)
    #spinner.stop()
    sys.stderr.write("\nCleaning complete.")
#######------------------END OF MAIN SECTION------------------------------######
################################################################################
