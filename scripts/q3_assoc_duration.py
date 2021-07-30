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

def calculate_contact_events_and_totals(col_items):
    '''
    calculate contact events and total events from the tracks

    return contact events, total events
    '''
    original_number_of_rows = len(col_items)
    #print((col_items <= 0.25).sum())
    contact_events = (col_items <= 0.25).sum() # use of `sum()` here is based on
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
        calculate_contact_events_and_totals, axis=0) #Note I had to remove the
    # `.transpose` step until later when I started trying to remove the 
    # empty column afert I stopped removing cases where total events was zero.
    # This below filter is what I originally was doing before told there should 
    # NOT be any actual tracks that have no events.
    # filter out rows where total events is 0.0 and then add a column
    # that is ratio contact events/ total events. To avoid division by zero is
    # why the filtering is done first
    #contact_df = contact_df.drop(
    #   contact_df[contact_df.total_events == 0.0].index)
    # After learning that, we invesitgated and found there was an empty column 
    # at the end of several files examine.  The column didn't even have a 
    # header. I switched to this filter on next line to drop those, which
    # should also eliminate any total event columns that are 0.0; however, 
    # making it target empty columns will highlight if there are cases with 
    # zero events in the columns where there is a 'shortest distance..' header.
    # Based on https://gist.github.com/aculich/fb2769414850d20911eb that I found 
    # by searching 'pandas read_csv skip empty columns'
    #contact_df = contact_df.dropna(axis='columns', how='all') # THIS SHOULD 
    # HAVE WORKED BECAUSE WORKED WITH TOY CSV TESTS BUT DIDN'T WITH REAL CSVs 
    #HERE!!
    # Found https://stackoverflow.com/a/43983654/8508004 to get rid of the 
    # EMPTY column at the end that begins with `Unnamed`
    contact_df = contact_df.loc[:, ~contact_df.columns.str.contains('^Unnamed')]
    contact_df = contact_df.transpose() #now that empty column handled transpose
    # calculate ratio of contact events to total events
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


# CATEGORIZE BASED ON DURATION INTERVALS & MAKE A BREAKDOWN OF CATEGORIES:
#------------------------------------------------------------------------------#
# Ratio intervals will be broken into quarters.
count_dfs = []
interval_col_name = 'ratio intervals'
for sample in collected_assoc_duration_dict_by_sample:
    df_for_counts = resulting_df.copy()
     # even though want quartiles for the intervals, don't want `pd.qcut` here, 
     # because "Qcut (quantile-cut) differs from cut in the sense that, in qcut, 
     # the number of elements in each bin will be roughly the same, but this 
     # will come at the cost of differently sized interval widths."-FROM 
     # https://www.geeksforgeeks.org/how-to-use-pandas-cut-and-qcut/
    df_for_counts[interval_col_name] = pd.cut(
        resulting_df[sample,"ratios"],  bins=[0, .25, .5, .75, 1.],
        labels=['0 <= 0.25','0.25 <= 0.5','0.5 <= 0.75','0.75 <= 1.0'])
    df_for_counts = df_for_counts.groupby(
        by=interval_col_name).size().reset_index(name=sample) # based on 
    # https://stackoverflow.com/a/32801170/8508004
    count_dfs.append(df_for_counts)
counts_df = pd.merge(*count_dfs,on=interval_col_name)
# make coloumns multiindex
#multi_tuples = [(
#    f'{interval_col_name}',' '), ('counts','wt'), ('counts','other')] # for 
# development
multi_tuples = [(
    f'{interval_col_name}',' ')] + [('counts',x) for x in counts_df.columns[1:]]
multi_cols = pd.MultiIndex.from_tuples(multi_tuples)
counts_df.columns=multi_cols
counts_file_name_prefix = (
    f"ratio_intervals_breakdown{now.strftime('%b%d%Y%H%M')}")
counts_df.to_excel(counts_file_name_prefix+'.xlsx')
counts_df.to_csv(counts_file_name_prefix+ '.tsv', sep='\t',index = False)
    

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
