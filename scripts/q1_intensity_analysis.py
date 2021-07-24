# q1_intensity_analysis.py
__author__ = "Wayne Decatur" #fomightez on GitHub
__license__ = "MIT"
__version__ = "0.1.0"


# q1_intensity_analysis.py by Wayne Decatur
# ver 0.1.0
#
#*******************************************************************************
# 
# PURPOSE: Processes data as advised in Q1 outlint
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

def write_string_to_file(s, fn):
    '''
    Takes a string, `s`, and a name for a file & writes the string to the file.
    '''
    with open(fn, 'w') as output_file:
        output_file.write(s)

def out2_stderr_n_log(s,log_file_text):
    '''
    Takes a string as input and sends it to the stderr as well as to a building
    string that will everntually get saved as a Log file.
    Also needs the Log file to be sent in because gets assigned within the
    function in order to add to it. Returns the modified `log_file_text`.
    '''
    sys.stderr.write(s)
    log_file_text += s
    return log_file_text



def chunk_string(string, chunk_size):
    """Return a list of n-sized chunks from string of letters."""
    return [string[i:i+chunk_size] for i in range(0, len(string),chunk_size)] 


def strip_off_first_line(fn,set_name,character_to_mark_set_name_end):
    '''
    This takes a name of a file & then uses the shell to remove the first line.
    In order to leave the input file intact, a new multi-sequence FASTA file
    is made and that is used in place of the one where the label was the first
    line. The set sample name extracted gets added to the file name.
    Removing first line based on 
    https://unix.stackexchange.com/questions/96226/delete-first-line-of-a-file
    '''
    name_for_f_without_first_line = (
        f"{set_name}{character_to_mark_set_name_end}set.fa")
    #!tail -n +2 {fn} >{name_for_f_without_first_line} 
    os.system(f"tail -n +2 {fn} >{name_for_f_without_first_line}")
    return name_for_f_without_first_line


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
collected_max_dict = {} #initialize a dictionary for collecting the data as it 
# is processed

for fn in csv_files:
    # make a dataframe from the CSV file 
    csv_df = pd.read_csv(fn, header=2)
    #collect the maximum in track column
    max_track_for_tracks = csv_df[csv_df.columns[1:]].max(axis=0).tolist()
    #store the list with the filename as key
    collected_max_dict[fn] = max_track_for_tracks

# MAKE A DATAFRAME FROM THE COLLECTED DATA:
#------------------------------------------------------------------------------#
#df = pd.DataFrame(collected_max_dict) # cannot do this directly like that 
# because the lists can be different length and get :
# 'ValueError: arrays must all be same length' Fix below based on 
# https://stackoverflow.com/a/40442094/8508004 , which makes it from a dict with
# uneven lists
df = pd.DataFrame.from_dict(collected_max_dict, orient='index')
df = df.transpose()

# SAVE THE DATAFRAME AS CSV and EXCEL:
#------------------------------------------------------------------------------#
now = datetime.datetime.now()
spreadsheet_name_prefix = f"intensity_collected{now.strftime('%b%d%Y%H%M')}"
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
