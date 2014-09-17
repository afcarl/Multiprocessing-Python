# homework3.py
# (C) Brendan J. Herger
# Analytics Master's Candidate at University of San Francisco
# 13herger@gmail.com
#
# Available under MIT License
# http://opensource.org/licenses/MIT
#
# *********************************
__author__ = 'bjherger'

# imports
# ###########################################
import bhUtilties
import hw3

bhUtilties.output_to_file()

# variables
# ###########################################

process_month = hw3.get_properties_from_list
separator = "\n**********************************************************"

# functions
# ###########################################
def get_list():
    pickle_file_name = "sec_list.pkl"
    list = bhUtilties.loadPickle(pickle_file_name)
    if not list:
        list = hw3.get_filetype_on_page("http://www.sec.gov/Archives/edgar/monthly/index.json")
        bhUtilties.savePickle(list, pickle_file_name)
    return list

def for_loop(funciton, list):
    return funciton(list)

def multi_map(funciton, list):
    return bhUtilties.multi_map(funciton, list)


def main():

    print "\nsort_1", separator
    list_to_work_on = get_list()[:]
    t_sort_1 = bhUtilties.timeItStart()
    for_loop(process_month, list_to_work_on)
    bhUtilties.timeItEnd(t_sort_1)

    print "\nmulti_sort", separator
    list_to_work_on = get_list()[:]
    t_multi_start = bhUtilties.timeItStart()
    multi_map(process_month, list_to_work_on)
    bhUtilties.timeItEnd(t_multi_start)


# main
############################################

if __name__ == "__main__":
    print "Begin Main"
    main()
    print "End Main"




