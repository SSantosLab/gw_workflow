#!/usr/bin/env python

import os
import time
import datetime
import numpy as np
##################################

def main():
    import csv
    import numpy as np
    import healpy as hp
    import healpy.pixelfunc
    import pandas as pd
    import string,sys,os,glob
    import fitsio
    from glob import glob
    
    datadir = '/data/des40.b/data/gaia/dr2/healpix'
    files = glob(datadir + "/*") # a list of filenames in the datadir
    done = glob('./*') # files already done and in the current directory
    for f in files:
        if f in done:
           continue 
        catalog = []
        healpixnum = f[f.rfind("_")+1:-5]

        d = fitsio.read('/%s'%f, columns=['SOURCE_ID','RA','DEC','PHOT_G_MEAN_MAG'])
        catalog.append(d)
        # assumes all pixels are unique
        catalog = np.concatenate(catalog)

        outfile="""GaiaOut%s.csv""" % healpixnum

        df=pd.DataFrame()
        good_data=[]

        df=pd.DataFrame(catalog.byteswap().newbyteorder(), index=range(catalog.size), columns=['SOURCE_ID','RA','DEC','PHOT_G_MEAN_MAG']) # byteswap because fits is big-endian, so swap byte order to native order
        good_data.append(df)

        chunk = pd.concat(good_data, ignore_index=True)
        chunk = chunk.sort_values(by=['RA'], ascending=True) # DataFrame.sort is deprecated
     
        datastd1= pd.DataFrame({'MATCHID':chunk['SOURCE_ID'],'RA':chunk['RA'],'DEC':chunk['DEC'],'WAVG_MAG_PSF':chunk['PHOT_G_MEAN_MAG']})

        col=["MATCHID", "RA","DEC", "WAVG_MAG_PSF"]

        datastd1.to_csv(outfile,columns=col,sep=',',index=False)

###############

if __name__ == "__main__":
    main()
