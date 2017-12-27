#!/usr/bin/env python
"""
    nobs_fs_table_query.py

    Example:
    
    nobs_fs_table_query.py --help

    nobs_fs_table_query.py --outputFile nobs_fs_table.csv \
                         --verbose 2
    
    """

##################################

def main():

    import argparse
    import time

    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--outputFile', help='name of an output file', default='output.csv')
    parser.add_argument('--verbose', help='verbosity level of output to screen (0,1,2,...)', default=0, type=int)
    args = parser.parse_args()

    if args.verbose > 0: print args

    # Run exposure query...
    status = nobs_fs_table_query(args)


##################################
# 

def nobs_fs_table_query(args):

    import numpy as np
    import pandas as pd
    import time
    import datetime
    from astroquery.utils.tap.core import TapPlus

    if args.verbose>0: 
        print 
        print '* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *'
        print 'nobs_fs_table_query'
        print '* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *'
        print 

    print datetime.datetime.now()

    # Extract info from argument list...
    outputFile=args.outputFile
    
    # Establish TAP connection to SkyMapper database...
    skyskymapper = TapPlus(url="http://skymappertap.asvo.nci.org.au/ncitap/tap")

    # Formulate the query...
    query = """SELECT image_id, count(*) as nobj FROM dr1.fs_photometry group by image_id"""
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
    nobs_fs_table = job.get_results()
    if args.verbose>0: 
        print "Retrieval completed:  ", datetime.datetime.now()

    # Sort table by image_id
    nobs_fs_table.sort('image_id')

    # Convert the results into a pandas data frame...
    #  (Could also use Table.to_pandas, but, unless careful,
    #   the image_id gets converted from an int64 to a float.)
    d = {'nobj':np.array(nobs_fs_table['nobj']), 'image_id':np.array(nobs_fs_table['image_id'])}
    df = pd.DataFrame(d)

    # Save dataframe to outputFile...
    if args.verbose>0: 
        print "File output start:      ", datetime.datetime.now()
    df.to_csv(outputFile,index=False)
    if args.verbose>0: 
        print "File output completed:  ", datetime.datetime.now()

    if args.verbose>0: print

    return 0


##################################

if __name__ == "__main__":
    main()

##################################
