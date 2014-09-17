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
import feedparser
import json
import multiprocessing
import pandas
import re
import sys
import urllib2
import urlparse

import numpy as np

# variables
# ###########################################

# functions
# ###########################################


def get_filetype_on_page(url, filetype="xml"):
    """
    Get a list of all links to the specified file type.
    file names can only contain [a-zA-Z0-9-_] characters
    :param url: url of page to read
    :param filetype: Extension to search for. Do not include dot
    :return: list of files, normalized to be absolute paths
    :rtype: list
    """
    # get page
    page = urllib2.urlopen(url).read()

    # get href tags, e.g. "href":"xbrlrss-2013-07.xml"
    match_list = re.findall(r"\"href\":\"[a-zA-Z0-9-_]*\." + filetype + "\"", page)

    # get filenames, e.g. xbrlrss-2013-07.xml
    match_list = [re.findall(r"[a-zA-Z0-9-_]*\." + filetype, local_match)[0] for local_match in match_list]

    # change partial url to absolute url
    match_list = [urlparse.urljoin(url, relative_url) for relative_url in match_list]
    match_list = list(set(match_list))
    return match_list


def get_properties_from_list(xml_url_list):
    """
    Traverses xml_url_list, attempts to get list of submission properties, with attributes:
        SIC code, company name, form type, filing date and link
    Attributes can be None
    :param xml_url_list: list of xml pages. This is highly specific to
        http://www.sec.gov/Archives/edgar/monthly/index.json
    :return: list of dictionaries, of the form [ { attribue : value }, ... ]
    :rtype: list
    """

    # variables
    submission_list = []

    # traverse each month
    for month_rss_url in xml_url_list:

        # scan each month's page
        try:
            feed = feedparser.parse(month_rss_url)
            for submission in feed['entries']:
                # variables
                submission_details = {}
                submission_details["SIC"] = submission.get('edgar_assignedsic', None)
                submission_details["company_name"] = submission.get('edgar_companyname', None)
                submission_details["form_type"] = submission.get('edgar_formtype', None)
                submission_details["filing_date"] = submission.get('edgar_filingdate', None)

                url = submission.get('links', None)
                if url and len(url) > 0:
                    url = url[0].get('href', None)

                submission_details["url"] = url

                # add to list of submissions
                submission_list.append(submission_details)

        except Exception, err:
            print err
            print "Error!"
    # write that progress has been made

    return submission_list


def flatten_list(list):
    """
    Flattens a list of lists. Non-recursive.
    :param list: list of form [ [...] , [...], ... ]
    :return: list of form [ ... ]
    """
    temp = []
    for elem in list:
        temp.extend(elem)
    return temp


def problem1(site="http://www.sec.gov/Archives/edgar/monthly/index.json"):
    """
    Gathers summary of all XBRL data within SEC EDGAR archives.
    Prints SIC code, company name, form type, filing date and link
    :param site: site to be searched. This is highly specific in format
    :return: pandas dataframe containing columns ['SIC', 'company_name', 'form_type', 'filing_date',  'url']
    """

    # get list of urls
    xml_url_list = get_filetype_on_page(site)[:3]

    #set up multiprocessing
    pool_size = multiprocessing.cpu_count() * 3
    pool = multiprocessing.Pool(processes=pool_size, maxtasksperchild=4)
    list_of_list_of_files = np.array_split(xml_url_list, pool_size * 2)

    print "\nLoading XML"
    print "0%", "." * len(list_of_list_of_files), "100%"
    print "0% ",

    # map, get properties for each subset of pages
    pool_outputs = pool.map(get_properties_from_list, list_of_list_of_files)

    # join threads
    pool.close()  # no more tasks
    pool.join()  # wrap up current tasks
    print "100%"
    print "\nCombining"
    # re-join output from pool into one list
    pool_outputs = flatten_list(pool_outputs)

    # convert to dataframe
    df = pandas.DataFrame(pool_outputs)

    # reorder columns
    cols = ['SIC', 'company_name', 'form_type', 'filing_date', 'url']
    df = df[cols]

    # sort
    df = df.sort(["SIC", "company_name"])
    print df

    # return
    return df


def makeCategory(df):
    """
    Format string to correct category
    :param df:
    :return:
    """
    # get value
    lower_bound = ((( df["Rank"] - 1 ) / 100) * 100) + 1
    upper_bound = lower_bound + 99

    # make pretty text
    lower_bound = "%03d" % lower_bound
    upper_bound = "%03d" % upper_bound

    return lower_bound + " to " + upper_bound


def problem2():
    """
    Generate dataframe of fortune 500 companies, with factors including:
        'Company', 'Profit', 'Rank', 'Revenue', 'Year'
    Additionally prints summed revenue, as aggregated by year, rank bucket of 100
    :return: dataframe containing 'Company', 'Profit', 'Rank', 'Revenue', 'Year'
    """

    # variables
    records = []

    # iterate through years
    for year in xrange(2000, 2010):

        # local variables
        year_dict = {}

        try:

            # hardcoded sec url
            url = "http://gomashup.com/json.php?fds=finance/fortune500/year/%d" % year

            # console output
            print "\n", year
            print 'Querying', url

            # get webpage, json information
            request = urllib2.Request(url)
            page = urllib2.urlopen(request)
            page = page.read()
            without_parans = page[1:-1]
            decoded = without_parans.decode('ASCII', 'ignore')

            # hardcoded replacement for incorrect json encoding
            decoded = decoded.replace("Toys R\"\" Us\"\"", "Toys R Us")
            results = json.loads(decoded, strict=False)['result']

            # iterate through results json
            for new_entry in results:

                # get new_rank, compare to old_rank
                company_name = new_entry.get("Company", None)
                new_rank = new_entry.get("Rank", None)

                old_entry = year_dict.get(company_name, {})
                old_rank = old_entry.get("Rank", str(sys.maxint))

                new_rank = int(new_rank) if new_rank.isdigit() else None
                old_rank = int(old_rank) if old_rank.isdigit() else sys.maxint

                # add if possible
                if new_rank < old_rank:
                    year_dict[company_name] = new_entry

        # Exceptions...
        except urllib2.URLError, e:
            print
            print 'Error: ', e
            sys.exit()

        # add to running total
        records.extend(year_dict.values())

    # create pandas dataframe
    df = pandas.DataFrame(records)
    df[['Profit', 'Rank', "Revenue", "Year"]] = df[['Profit', 'Rank', "Revenue", "Year"]].convert_objects(
        convert_numeric=True)

    # add bucks
    df["Bucket"] = df.apply(makeCategory, axis=1)

    # get aggregrate by year, bucket
    agg_df = df.groupby(["Year", "Bucket"]).sum()

    # make pretty
    agg_df.drop('Rank', axis=1, inplace=True)
    agg_df.drop('Profit', axis=1, inplace=True)
    agg_df.columns = ["Total Revenue"]

    # output to stream
    print agg_df

    return df


# main
############################################

if __name__ == "__main__":
    print "Begin Main"
    problem1()
    problem2()
    print "End Main"

