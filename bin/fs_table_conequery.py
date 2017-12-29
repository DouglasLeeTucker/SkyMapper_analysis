#!/usr/bin/env python
"""
    fs_table_conequery.py

    Example:
    
    fs_table_conequery.py --help

    fs_table_conequery.py --radeg 194.53 --decdeg -87.33 --radiusdeg 1.5 --outputFile fs_table.csv --verbose 2
    
    """

##################################

def main():

    import argparse
    import time

    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--radeg', help='ra of the center of the cone search (in deg)', default=0.00, type=float)
    parser.add_argument('--decdeg', help='dec of the center of the cone search (in deg)', default=0.00, type=float)
    parser.add_argument('--radiusdeg', help='radius of the cone search (in deg)', default=1.00, type=float)
    parser.add_argument('--outputFile', help='name of an output file', default='output.csv')
    parser.add_argument('--verbose', help='verbosity level of output to screen (0,1,2,...)', default=0, type=int)
    args = parser.parse_args()

    if args.verbose > 0: print args

    # Run exposure query...
    status = fs_table_conequery(args)


##################################
# 

def fs_table_conequery(args):

    import numpy as np
    import pandas as pd
    import math
    import time
    import datetime
    from astroquery.utils.tap.core import TapPlus

    if args.verbose>0: 
        print 
        print '* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *'
        print 'fs_table_conequery'
        print '* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *'
        print 

    print datetime.datetime.now()

    # Extract info from argument list...
    radeg0=args.radeg
    decdeg0=args.decdeg
    radiusdeg=args.radiusdeg
    outputFile=args.outputFile

    # Read in nobs_fs_file...
    nobs_fs_file = 'nobs_fs_table.csv'
    df_nobs_fs = pd.read_csv(nobs_fs_file)

    # Read in images_file...
    images_file = 'images_table.csv'
    df_images = pd.read_csv(images_file)

    # Merge df_nobs_fs and df_images on image_id...
    df_images_nobs = df_nobs_fs.merge(df_images, on=['image_id']).sort_values(by='image_id', ascending=True).reset_index(drop=True)

    # Calculate separation between center of cone
    # and ra,dec of each image in df_images_nobs
    rarad0 = math.radians(radeg0)
    decrad0 = math.radians(decdeg0)

    df_images_nobs.loc[:,'rarad'] = np.radians(df_images_nobs.loc[:,'ra'])
    df_images_nobs.loc[:,'declrad'] = np.radians(df_images_nobs.loc[:,'decl'])
    
    df_images_nobs.loc[:,'cosSep'] = math.sin(decrad0)*np.sin(df_images_nobs.loc[:,'declrad']) + \
        math.cos(decrad0)*np.cos(df_images_nobs.loc[:,'declrad'])*np.cos(rarad0 - df_images_nobs.loc[:,'rarad'])
    df_images_nobs.loc[:,'sepDeg'] = np.degrees(np.arccos(df_images_nobs.loc[:,'cosSep']))
    
    # Find which image_id's lie within the cone search radius...
    mask = (df_images_nobs.loc[:,'sepDeg'] < radiusdeg)
    if args.verbose > 0:
        print df_images_nobs.loc[mask, ['image_id','filter','ra','decl','sepDeg']]
    
    # Establish TAP connection to SkyMapper database...
    skymapper = TapPlus(url="http://skymappertap.asvo.nci.org.au/ncitap/tap")

    # Loop over image_id's...
    i = 0
    for image_id in df_images_nobs.loc[mask, 'image_id']:

        # Formulate the query...
        query = """SELECT * FROM dr1.fs_photometry where image_id=%d""" % (image_id)
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
        fs_table = job.get_results()
        if args.verbose>0: 
            print "Retrieval completed:  ", datetime.datetime.now()

        # Sort table by ra_img
        # fs_table.sort('ra_img')

        # "Append" does not seem to work for stropy table.write.
        # For now, save each file individually....
        # Save table to outputFile...
        #if args.verbose>0: 
        #    print "File output start:      ", datetime.datetime.now()
        #if i == 0:
        #    fs_table.write(outputFile)
        #else:
        #    fs_table.write(outputFile, append=True)
        #if args.verbose>0: 
        #    print "File output completed:  ", datetime.datetime.now()
        tmpOutputFile = """%s.%d.csv""" % (outputFile, image_id) 
        if args.verbose>0: 
            print "File output start:      ", datetime.datetime.now()
        fs_table.write(tmpOutputFile)
        if args.verbose>0: 
            print "File output completed:  ", datetime.datetime.now()

        i = i + 1

        if args.verbose>0: print


    return 0


##################################

if __name__ == "__main__":
    main()

##################################
