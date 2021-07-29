# q3_assoc_duration.py
__author__ = "Wayne Decatur" #fomightez on GitHub
__license__ = "MIT"
__version__ = "0.1.0"


# q3_assoc_duration.py by Wayne Decatur
# ver 0.1.0
#
#*******************************************************************************
# 
# PURPOSE: Processes data as advised in Q3 outline
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

def calculate_contact_events_and_totals(col_items):
    '''
    calculate contact events and total events from the tracks

    return contact events, total events
    '''
    original_number_of_rows = len(col_items)
    #print((col_items > 0.25).sum())
    contact_events = (col_items > 0.25).sum() # use of `sum()` here is based on
    # DSM's comment below https://stackoverflow.com/a/23833925/8508004 ; it 
    # works, wheras count doesn't, because the slection part is making a list of 
    # booleans as to whether condition holds and `1` and `True` equate.
    col_items['contact_events'] = contact_events
    total_events = (
        col_items[:original_number_of_rows].dropna()).count()
    col_items['total_events'] = total_events
    #col_items['ratios'] = contact_events/total_events #leaving adding rations
    # until have a dataframe so
    return col_items[(original_number_of_rows-len(col_items)):]

def calculate_ratio(items):
    '''
    takes a iterable of items and calculates a ratio of the first value divided
    by the second
    '''
    return items[0]/float(items[1])

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
collected_assoc_duration_dict = {} #initialize a dictionary for collecting the 
# data as it is processed


for fn in csv_files:
    # make a dataframe from the CSV file 
    csv_df = pd.read_csv(fn, header=2)
    #collect the number of contact events and total events from each track
    contact_df = csv_df[csv_df.columns[1:]].apply(
        calculate_contact_events_and_totals, axis=0).transpose()
    # filter out rows where total events is 0.0 and then add a column
    # that is ratio contact events/ total events. To avoid division by zero is
    # why the filtering is done first
    contact_df = contact_df.drop(
        contact_df[contact_df.total_events == 0.0].index)
    contact_df['ratios'] = contact_df.apply(calculate_ratio, axis=1)
    #store the dataframe with the filename, minus the .csv extension, as key
    collected_assoc_duration_dict[fn[:-4]] = contact_df

# COMBINE COLLECTED DATA BASED ON SAMPLE ID:
#------------------------------------------------------------------------------#
from collections import defaultdict
collected_assoc_duration_dict_by_sample = defaultdict(pd.DataFrame)
for fn,assoc_df in collected_assoc_duration_dict.items():
    collected_assoc_duration_dict_by_sample[extract_sample_id(
        fn).upper()] = pd.concat(
        [collected_assoc_duration_dict_by_sample[extract_sample_id(
        fn).upper()],assoc_df], ignore_index=True)


# MAKE SINGLE DATAFRAME BY COMBINING DATA, ADDING SAMPLE IDs AS COLUMN HEADINGS:
#------------------------------------------------------------------------------#
# first iterate on collected_assoc_duration_dict_by_sample adding the sample ids
# as column heading in the individual dataframes by adding an upper level
#  multiindex column name that is sample id. Because once that is added to the
# complete dataframe for each it is just a matter of concatenating them side by
# side
for sample,ind_df in collected_assoc_duration_dict_by_sample.items():
    #multi_tuples = [(
    #    'WT,'contact_events'), ('WT','total_events'), ('WT,'ratios')] # for 
    # development
    multi_tuples = [(
        sample,ind_df.columns[0]), (sample,ind_df.columns[1]), 
        (sample,ind_df.columns[2])]
    multi_cols = pd.MultiIndex.from_tuples(multi_tuples)
    ind_df.columns=multi_cols
    collected_assoc_duration_dict_by_sample[sample] = ind_df
# now to combine
resulting_df = pd.concat(
    collected_assoc_duration_dict_by_sample.values(),axis=1)


# SAVE THE RESULTING DATAFRAME AS CSV and EXCEL:
#------------------------------------------------------------------------------#
now = datetime.datetime.now()
spreadsheet_name_prefix = f"assoc_duration_collected{now.strftime('%b%d%Y%H%M')}"
resulting_df.to_excel(spreadsheet_name_prefix+'.xlsx')
resulting_df.to_csv(spreadsheet_name_prefix+ '.tsv', sep='\t',index = False)
    


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
