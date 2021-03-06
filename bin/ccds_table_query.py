#!/usr/bin/env python
"""
    ccds_table_query.py

    Example:
    
    ccds_table_query.py --help

    ccds_table_query.py --outputFile ccds_table.csv --object_id_lo 0 --object_id_hi 499999 --verbose 2
    
    """

##################################

def main():

    import argparse
    import time

    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--outputFile', help='name of an output file', default='output.csv')
    parser.add_argument('--ccd', help='ccd number (1-32)', default=1, type=int)
    parser.add_argument('--verbose', help='verbosity level of output to screen (0,1,2,...)', default=0, type=int)
    args = parser.parse_args()

    if args.verbose > 0: print args

    # Run exposure query...
    status = ccds_table_query(args)


##################################
# 

def ccds_table_query(args):

    import numpy as np
    import pandas as pd
    import time
    import datetime
    from astroquery.utils.tap.core import TapPlus

    if args.verbose>0: 
        print 
        print '* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *'
        print 'ccds_table_query'
        print '* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *'
        print 

    print datetime.datetime.now()

    # Extract info from argument list...
    outputFile = args.outputFile
    ccd = args.ccd
    
    # Establish TAP connection to SkyMapper database...
    skymapper = TapPlus(url="http://skymappertap.asvo.nci.org.au/ncitap/tap")

    # Formulate the query...
    query = """SELECT * FROM dr1.ccds where ccd=%d""" % (ccd)
    if args.verbose>0: 
        print "Query:  ", query
    
    # Submit the query as an asynchronous job...
    if args.verbose>0: 
        print "Query start:      ", datetime.datetime.now()
    job = skymapper.launch_job_async(query)
    if args.verbose>0: 
        print "Query completed:  ", datetime.datetime.now()

    # Retrieve the results as an astropy Table...
    if args.verbose>0: 
        print "Retrieval start:      ", datetime.datetime.now()
    ccds_table = job.get_results()
    if args.verbose>0: 
        print "Retrieval completed:  ", datetime.datetime.now()

    # Sort table by image_id
    ccds_table.sort('image_id')

    # Save table to outputFile...
    if args.verbose>0: 
        print "File output start:      ", datetime.datetime.now()
    ccds_table.write(outputFile)
    if args.verbose>0: 
        print "File output completed:  ", datetime.datetime.now()

    if args.verbose>0: print

    return 0


##################################

if __name__ == "__main__":
    main()

##################################
