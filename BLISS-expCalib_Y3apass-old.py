#!/usr/bin/env python
#
# Version from 10/09/2017 according to Sahar rounding was changed from 357 to 350.
#
#
"""
    expCalib.py THIS VERSION ONLY works for Carina2-3
    Express Calibration, 
    This code will estimate the zero-points   
    v.3 Apr20, 2016
    NOW using apass_2massInDES.sorted.csv via APASS/2MASS.
    v.2 Feb25, 2016:
    Now use APASS Dr7 and tested with ALex.. MagsLite
    Date Thu Sep 24, 2015
    NOTE that this catalog is only for the officical DES-foot print, some ccd will have no ZP
    Example:   
    expCalib_Y3apass.py --help

    BLISS-expCalib_Y3apass.py -s db-desoper --expnum 475956 --reqnum 04 --attnum 11 --ccd 1
    
    """
import os
import time
import datetime
import numpy as np
##################################

def main():
    import argparse
    import time
    print " Start with BLISS-expCalib.py \n"
    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--expnum', help='expnum is queried', default=350245, type=int)
    parser.add_argument('--reqnum', help='reqnum is queried', default=922, type=str)
    parser.add_argument('--attnum', help='attnum is queried', default=1, type=int)
    parser.add_argument('--ccd', help='ccd is queried', default=1, type=int)
    parser.add_argument('--magType', help='mag type to use (mag_psf, mag_auto, mag_aper_8, ...)', default='mag_psf')
    parser.add_argument('--sex_mag_zeropoint', help='default sextractor zeropoint to use to convert fluxes to sextractor mags (mag_sex = -2.5log10(flux) + sex_mag_zeropoint)', type=float, default=25.0)
    parser.add_argument('--verbose', help='verbosity level of output to screen (0,1,2,...)', default=0, type=int)
    parser.add_argument('--debug', help='debugging option', dest='debug', action='store_true', default=False)
                        
    args = parser.parse_args()
                        
    if args.verbose > 0: print args

#    sys.exit()

    #--
    #Get FITS TABLE ONLY if needed
#    status = Wget_data_home(args)

    #
    #-- ADDED NEW
    #GET STD from APASS-DR9 and 2MASS in DES-footprint
    #status= getallccdfromAPASS9(args)

    #-- ADDED NEW
    #GET STD from APASS-DR9 and 2MASS FULL SKY
#    status= getallccdfromAPASS92MASS(args)
    status= getallccdfromGAIA(args)

    #-- ADDED NEW
    status = doset(args)

    #WHEN NEEDED
    #plot ra,dec of Sex-vs Y2Q1 for each CCD
    if args.verbose >0 :
        status = plotradec_sexvsY2Q1(args)

    #--
    #Estimate 3sigma Clipped Zeropoint for each CCD
    status = sigmaClipZP(args)

    #-
    status=sigmaClipZPallCCDs(args)

    #-
    status=ZP_OUTLIERS(args)

    #--
    status=Onefile(args)

    #--
    #plot ra,dec of matched stars for ALL CCDs
    " Comment this line for grid production "
#    status = plotradec_ZP(args)



##################################
# Get data from  PROD tables EXPOSURE IMAGE, WDF, and CATALOG,
# then Convert the Fits table to csv and save it


def doset(args):
    import os,glob
    import csv
    import time
    import healpy as hp    
    import sys
    
    if args.verbose >0 : print args

    # delete previous *Obj.csv files
    oldfiles = glob.glob("*Obj.csv")
    for f in oldfiles:
        if os.path.isfile(f):
            os.remove(f)
        else:
            print "Old object file not deleted because it could not be found."

    catlistFile="D%08d_r%sp%s_red_catlist.csv" % (args.expnum,str(args.reqnum),str(args.attnum))
#    print " looking for file %s in local directory \n" % catlistFile
    if not os.path.exists(catlistFile):
        print '%s does not seem to exist... exiting now...' % catlistFile
        sys.exit(1)
#    print " file %s found \n" %catlistFile
    data=np.genfromtxt(catlistFile,dtype=None,delimiter=',',names=True)
    # get only data for correct CCD
    correctccd = (data['CCDNUM'] == args.ccd)
    data = data[ correctccd ]
#    print " before loop \n"    

    for i in range(data['FILENAME'].size):
        # to avoid error when array is not 2D because it only has one entry
        # different from line 417 because this is a numpy array, not pandas
        filetocheck = data['FILENAME']
        databand = data['BAND']
        dataracent = data['RA_CENT']
        datarac1 = data['RAC1']
        datarac2 = data['RAC2']
        datarac3 = data['RAC3']
        datarac4 = data['RAC4']
        datadeccent = data['DEC_CENT']
        datadec1 = data['DECC1']
        datadec2 = data['DECC2']
        datadec3 = data['DECC3']
        datadec4 = data['DECC4']
        if data['FILENAME'].size>1:
            filetocheck = data['FILENAME'][i]
            databand = data['BAND'][i]
            dataracent = data['RA_CENT'][i]
            datarac1 = data['RAC1'][i]
            datarac2 = data['RAC2'][i]
            datarac3 = data['RAC3'][i]
            datarac4 = data['RAC4'][i]
            datadeccent = data['DEC_CENT'][i]
            datadec1 = data['DECC1'][i]
            datadec2 = data['DECC2'][i]
            datadec3 = data['DECC3'][i]
            datadec4 = data['DECC4'][i]

        filetocheck = filetocheck.astype(str)
        # problem with filetocheck, databand being numpy arrays
        if isinstance(filetocheck, np.ndarray):
            filetocheck = filetocheck[0]
        if isinstance(databand, np.ndarray):
            databand = databand[0]
#        filetocheck = np.array2string(filetocheck).strip('\'') # problem with filetocheck being a numpy array
#        filetocheck = filetocheck[filetocheck.find('\'')+1:] # hacky fix in case of unicode encoding
#        filetocheck = filetocheck[:-2] # hacky fix in case of unicode encoding
        if os.path.isfile(filetocheck):
            Read_Sexcatalogfitstocsv(args,filetocheck,databand)

            minra=min(dataracent,datarac1,datarac2,datarac3,datarac4)
            maxra=max(dataracent,datarac1,datarac2,datarac3,datarac4)
            mindec=min(datadeccent,datadec1,datadec3,datadec4)
            maxdec=max(datadeccent,datadec1,datadec3,datadec4)

            rac  = dataracent
            decc = datadeccent
            
            desipixc=getipix(128,dataracent, datadeccent)
            desipix1=getipix(128,datarac1,datadec1)
            desipix2=getipix(128,datarac2,datadec2)
            desipix3=getipix(128,datarac3,datadec3)
            desipix4=getipix(128,datarac4,datadec4)

            # get midpoints
            desipix12=getipix(128,datarac1,datadeccent)
            desipix23=getipix(128,dataracent,datadec2)
            desipix34=getipix(128,datarac3,datadeccent)
            desipix14=getipix(128,dataracent,datadec4)

            desipixlist= desipixc,desipix1,desipix2,desipix3,desipix4,desipix12,desipix23,desipix34,desipix14
            desipixlist=uniqlist(desipixlist)
        
            ra1=[];ra2=[];dec1=[];dec2=[]
            matchlistout="""%s_match.csv""" % (filetocheck)
            objlistFile ="""%s_Obj.csv"""   % (filetocheck)
            stdlistFile ="""%s_std.csv"""   % (filetocheck)

            if not os.path.isfile(objlistFile):
                print '%s does not seem to exist... exiting now...' % objlistFile            
                sys.exit(1)

            if not os.path.isfile(stdlistFile):
                print '%s does not seem to exist... exiting now...' % stdlistFile
                sys.exit(1)

            stdracol=1
            stddeccol=2
            obsracol=1
            obsdeccol=2 
            matchTolArcsec=1.0 #1.0arcsec
            verbose=2
            matchSortedStdwithObsCats(stdlistFile,objlistFile,matchlistout,stdracol,stddeccol,obsracol,obsdeccol,matchTolArcsec,verbose)
    print "after the loop \n"
            

    return 0
      
##################################
#get_data_home for NOW it is for all CCDs:
#
def Wget_data_home(args):
    import csv
#    from despyserviceaccess import serviceaccess
    import os
    import glob
    import sys

    catname="""D%08d_%s_%s_r%sp%1d_fullcat.fits""" % (args.expnum,"%","%",args.reqnum,args.attnum)
    myfile="""D%08d_*_r%sp%1d_fullcat.fits""" % (args.expnum,args.reqnum,args.attnum)

    #Check first if file exists...
    if  glob.glob(myfile):
        #Print '%s does seem to exist... exiting now...' % catname
        print "relevant cat files already exist in the current directory... no need to wget..."
        #sys.exit(1)
        return 1
    else:
        print "relevant cat files are not in directory... wgetting them from archive..."

        sys.exit(1)
    
##################################
# Quick Read SEX_table filemane_fullcat.fits then select subsame
# and write it as filemane_fullcat.fits_Obj.csv
def Read_Sexcatalogfitstocsv(args,fitsname,band):

    import fitsio
    import string
    import numpy as np
    import math
    import csv

    catFilename=fitsname
    outFile="""%s_Obj.csv""" % (catFilename)

    extension=2
    hdr = ["OBJECT_NUMBER","RA","DEC","MAG","MAGERR","ZEROPOINT","MAGTYPE","BAND"]

    magType = args.magType.upper()
    magType = magType.strip()
    fluxType = magType.replace('MAG','FLUX')
    fluxerrType = magType.replace('MAG','FLUXERR') 

    SEXdata=[]
    columns=['NUMBER','ALPHAWIN_J2000','DELTAWIN_J2000',fluxType,fluxerrType,'SPREAD_MODEL','SPREADERR_MODEL','FWHM_WORLD', 'CLASS_STAR', 'FLAGS']

    Fdata = fitsio.read(catFilename,  columns=columns, ext=extension)[:]
    #w0=( Fdata['FLUX_PSF'] > 2000) _OLD  
    w0=( Fdata['FLUX_PSF'] > 1000)  #NEW
    w1=( Fdata['FLAGS'] <= 3)
    #w2=( (Fdata['CLASS_STAR'] > 0.8 ) | (np.abs(Fdata['SPREAD_MODEL'] + 3.*Fdata['SPREADERR_MODEL'] <0.003 )))
    # NEW May16,16
    w2=( (Fdata['CLASS_STAR'] > 0.8 ) & (Fdata['SPREAD_MODEL']  <0.01 ) )

    SEXdata = Fdata[w0 & w1 & w2]
    SEXdata = SEXdata[np.argsort(SEXdata['ALPHAWIN_J2000'])]
    fwhm_arcsec=3600.*SEXdata['FWHM_WORLD']
    mag= -2.5*np.log10(SEXdata[fluxType]) + args.sex_mag_zeropoint
    magerr = (2.5/math.log(10.))*(SEXdata[fluxerrType]/SEXdata[fluxType])
    zeropoint=args.sex_mag_zeropoint*(SEXdata[fluxType]/SEXdata[fluxType])
  
    with open(outFile,'w') as csvFile:
            writer = csv.writer(csvFile,delimiter=',',  quotechar='|',
                                lineterminator='\n', quoting=csv.QUOTE_MINIMAL)

            writer.writerow(hdr)
            line=[]
            for i in range(SEXdata.size):
                line=SEXdata['NUMBER'][i],SEXdata['ALPHAWIN_J2000'][i], SEXdata['DELTAWIN_J2000'][i], mag[i], magerr[i], zeropoint[i], magType,band 
                writer.writerow(line)

    SEXdata=[]
    Fdata=[]

##################################
def radec2thetaphi(ra, dec):
    import numpy as np
    return (90-dec)*np.pi/180., ra*np.pi/180.

##################################
#for DES--nside=128
def getipix(nside,ra,dec):
    import healpy as hp
    #nside=128
    theta, phi = radec2thetaphi(ra, dec)
    ipix = hp.pixelfunc.ang2pix(nside, theta, phi, nest=True)
    return ipix

##################################
#Uniq List with order preserving
def uniqlist(seq): 
   noDupes = []
   [noDupes.append(i) for i in seq if not noDupes.count(i)]
   return noDupes

###################################
#NEW  June 1,2018 
#This is a FULL SKY 
#/data/des40.b/data/gaia/dr2
#################################
def getallccdfromGAIA(args):
    import csv
    import numpy as np
    import healpy as hp
    import healpy.pixelfunc
    import pandas as pd
    import string,sys,os,glob
    import fitsio

##### UNPACK IMAGE ###########

    #print NEED Round RA
    catlistFile="""D%08d_r%sp%s_red_catlist.csv""" % (args.expnum,str(args.reqnum),str(args.attnum))
    print "looking for file %s \n" % catlistFile
    if not os.path.exists(catlistFile):
        print '%s does not seem to exist... exiting now...' % catlistFile
        sys.exit(1)
#    print " found file %s \n" % catlistFile
    data=pd.read_csv(catlistFile)
#    print " the file is read  unpack data \n"

    # unpack image data
    BAND=data['BAND'][0]
   
    # check image standard deviation 
    stdRA = np.std(data['RA_CENT'])
    if ( stdRA >20 ) :
        data['RA_CENT']=[roundra(x) for x in data['RA_CENT']]
        data['RAC1']   =[roundra(x) for x in data['RAC1']]
        data['RAC2']   =[roundra(x) for x in data['RAC2']]
        data['RAC3']   =[roundra(x) for x in data['RAC3']]
        data['RAC4']   =[roundra(x) for x in data['RAC4']]

    # get image limits
    minra=min(min(data['RA_CENT']),min(data['RAC1']),min(data['RAC2']),min(data['RAC3']),min(data['RAC4']))-0.1
    mindec=min(min(data['DEC_CENT']),min(data['DECC1']),min(data['DECC2']),min(data['DECC3']),min(data['DECC4']))-0.1
    maxra=max(max(data['RA_CENT']),max(data['RAC1']),max(data['RAC2']),max(data['RAC3']),max(data['RAC4']))+0.1
    maxdec=max(max(data['DEC_CENT']),max(data['DECC1']),max(data['DECC2']),max(data['DECC3']),max(data['DECC4']))+0.1

    # read pixel from catalog
    # using only the row for the current CCD
    correctccd = (data['CCDNUM'] == args.ccd)
    data = data[ correctccd ]
    ra=data.iloc[0]['RA_CENT'] # in degrees; use iloc to get single cell
    dec=data.iloc[0]['DEC_CENT'] # in degrees
    nside=32
    radius=np.radians(0.2)
    #vec = hp.pixelfunc.ang2vec(ra,dec,lonlat=True) # for new healpy 1.11.0 and python 2.7.15

    # FOR OLD HEALPY 1.5dev
    theta, phi = radec2thetaphi(ra, dec)
    vec = hp.pixelfunc.ang2vec(theta, phi)

########## GET CORRESPONDING DATA FROM GAIA CATALOG ##########

    # List of nside=32 healpix pixel around specific ra,dec
    pix = hp.query_disc(nside,vec,radius,inclusive=True)

    datadir = '/cvmfs/des.osgstorage.org/stash/ALLSKY_STARCAT/GAIA_DR2'
    catalog = []
    for p in pix:
        prefix=p/100
        #d = fitsio.read(datadir+'/GaiaSource_%05d.fits'%p, columns=['SOURCE_ID','RA','DEC','PHOT_G_MEAN_MAG'])
        d = pd.read_csv(datadir+'/GaiaOut%03dXX/GaiaOut%05d.csv'%(prefix, p))
        catalog.append(d)
    # assumes all pixels are unique
    catalog = np.concatenate(catalog)
    rows, cols = catalog.shape

    outfile="""STD%s""" % catlistFile

    df=pd.DataFrame()
    good_data=[]
    BANDname=BAND+"_des"
    names=["MATCHID","RAJ2000_2mass","DEJ2000_2mass",BANDname]

    # this line is no longer needed because catalog is read in with pandas instead of fitsio
    #df=pd.DataFrame(catalog.byteswap().newbyteorder(), index=range(catalog.size), columns=['SOURCE_ID','RA','DEC','PHOT_G_MEAN_MAG']) # byteswap because fits is big-endian, so swap byte order to native order
    df=pd.DataFrame(catalog, index=range(rows), columns=['SOURCE_ID','RA','DEC','PHOT_G_MEAN_MAG'])
    good_data.append(df)

    # bounds checking; eliminate pixels not within current image (whole image)
    chunk = pd.concat(good_data, ignore_index=True)
    chunk = chunk.sort(['RA'], ascending=True) # DataFrame.sort is deprecated
    w1 = ( chunk['RA'] > minra )
    w2 = ( chunk['RA'] < maxra )
    w3 = ( chunk['DEC'] > mindec )
    w4 = ( chunk['DEC'] < maxdec )
    w5 = ( chunk['PHOT_G_MEAN_MAG'] >0 )
    
    datastd = chunk[ w1 &  w2 & w3 & w4 & w5 ]
    datastd1= pd.DataFrame({'MATCHID':datastd['SOURCE_ID'],'RA':datastd['RA'],'DEC':datastd['DEC'],'WAVG_MAG_PSF':datastd['PHOT_G_MEAN_MAG']})

    col=["MATCHID", "RA","DEC", "WAVG_MAG_PSF"]

    datastd1.to_csv(outfile,columns=col,sep=',',index=False)

    hdr=["MATCHID","RA","DEC","wavg_mag_psf"]

    # per CCD
    # filtering image pixel set into CCD sets

    for i in range(data['RA_CENT'].size):
        # adjust naming in case array is not 2d because it only contains one element
        #filetocheck = data['FILENAME'] # this line was writing a funky csv file
        #databand = data['BAND']
        #if data['FILENAME'].size>1:

        # no checks like elsewhere because this is a pandas df not numpy array
        #filetocheck = data['FILENAME'][i]
        filetocheck = data.iloc[0]['FILENAME']
        databand = data.iloc[0]['BAND']
        dataracent = data.iloc[0]['RA_CENT']
        datarac1 = data.iloc[0]['RAC1']
        datarac2 = data.iloc[0]['RAC2']
        datarac3 = data.iloc[0]['RAC3']
        datarac4 = data.iloc[0]['RAC4']
        datadeccent = data.iloc[0]['DEC_CENT']
        datadec1 = data.iloc[0]['DECC1']
        datadec2 = data.iloc[0]['DECC2']
        datadec3 = data.iloc[0]['DECC3']
        datadec4 = data.iloc[0]['DECC4']

        # calculate edges of image
        stdlistFile ="""%s_std.csv"""   % (filetocheck)
        minra=min(dataracent,datarac1,datarac2,datarac3,datarac4)-.1
        maxra=max(dataracent,datarac1,datarac2,datarac3,datarac4)+.1
        mindec=min(datadeccent,datadec1,datadec2,datadec3,datadec4)-.1
        maxdec=max(datadeccent,datadec1,datadec2,datadec3,datadec4)+.1
        w1 = ( datastd1['RA'] > minra ) ;w2 = ( datastd1['RA'] < maxra )
        w3 = ( datastd1['DEC'] > mindec );w4 = ( datastd1['DEC'] < maxdec )
        df = datastd1[ w1 &  w2 & w3 & w4 ].sort(['RA'], ascending=True)

##### OUTPUT TO CSV ########

        df.to_csv(stdlistFile,columns=col,sep=',',index=False)

###################################
#NEW  July 14,2016 
#### REPLACED by getacllccdfromGAIA on June 1, 2018

#This is a FULL SKY 
#/data/des20.b/data/sallam/pyPSM_Year2/TWOMASS/ALL-2MASS/2arcsec 
#catalog now in stash cache in dcache
# /pnfs/des//persistent/stash/ALLSKY_STARCAT/....
#apass_TWO_MASS_*.csv  in totall 768 files  
#filters are u_des,g_des,r_des,i_des,z_des,Y_des
#################################
def getallccdfromAPASS92MASS(args):
    import csv
    import numpy as np
    import pandas as pd
    import string,sys,os,glob

    #print NEED Round RA
    catlistFile="""D%08d_r%sp%s_red_catlist.csv""" % (args.expnum,str(args.reqnum),str(args.attnum))
    print "looking for file %s \n" % catlistFile
    if not os.path.exists(catlistFile):
        print '%s does not seem to exist... exiting now...' % catlistFile
        sys.exit(1)
#    print " found file %s \n" % catlistFile
    data=pd.read_csv(catlistFile)
#    print " the file is read  unpack data \n"
    BAND=data['BAND'][0]
    data['desipixc']=getipix(8,data['RA_CENT'], data['DEC_CENT'])
    data['desipix1']=getipix(8,data['RAC1'],data['DECC1']) 
    data['desipix2']=getipix(8,data['RAC2'],data['DECC2']) 
    data['desipix3']=getipix(8,data['RAC3'],data['DECC3']) 
    data['desipix4']=getipix(8,data['RAC4'],data['DECC4']) 

    data['desipix12']=getipix(8,data['RAC1'],data['DEC_CENT']) 
    data['desipix23']=getipix(8,data['RA_CENT'],data['DECC2']) 
    data['desipix34']=getipix(8,data['RAC3'],data['DEC_CENT']) 
    data['desipix14']=getipix(8,data['RA_CENT'],data['DECC4']) 
#    print " first step \n"
    desipixlist= pd.unique(data[['desipixc','desipix1','desipix2','desipix3','desipix4','desipix12','desipix23','desipix34','desipix14']].values.ravel())
#    print desipixlist
    desipixlist=uniqlist(desipixlist)
    stdRA = np.std(data['RA_CENT'])
    if ( stdRA >20 ) :
        data['RA_CENT']=[roundra(x) for x in data['RA_CENT']]
        data['RAC1']   =[roundra(x) for x in data['RAC1']]
        data['RAC2']   =[roundra(x) for x in data['RAC2']]
        data['RAC3']   =[roundra(x) for x in data['RAC3']]
        data['RAC4']   =[roundra(x) for x in data['RAC4']]

#    print " stdRA=%f \n" % stdRA    
#    if ( stdRA <=20 ) :
#        print " Something goes wrong  stdRA=%f exiting \n" % stdRA
#        sys.exit(1)
#    print " second step \n"
    minra=min(min(data['RA_CENT']),min(data['RAC1']),min(data['RAC2']),min(data['RAC3']),min(data['RAC4']))-0.1
    mindec=min(min(data['DEC_CENT']),min(data['DECC1']),min(data['DECC2']),min(data['DECC3']),min(data['DECC4']))-0.1
    maxra=max(max(data['RA_CENT']),max(data['RAC1']),max(data['RAC2']),max(data['RAC3']),max(data['RAC4']))+0.1
    maxdec=max(max(data['DEC_CENT']),max(data['DECC1']),max(data['DECC2']),max(data['DECC3']),max(data['DECC4']))+0.1

#    print minra,maxra, mindec,maxdec

    outfile="""STD%s""" % catlistFile

    df=pd.DataFrame()
    good_data  = []
    BANDname=BAND+"_des"
    names=["MATCHID","RAJ2000_2mass","DEJ2000_2mass",BANDname]

    for i in  desipixlist:
        #myfile="""/data/des20.b/data/sallam/pyPSM_Year2/TWOMASS/ALL-2MASS/2arcsec/apass_TWO_MASS_%d.csv""" %i
#        print "before copying file %d from stash " % i
        myfile="""/pnfs/des/persistent/stash/ALLSKY_STARCAT/apass_TWO_MASS_%d.csv""" %i
#        myfile1 = "/cvmfs/des.osgstorage.org/stash/ALLSKY_STARCAT/apass_TWO_MASS_%d.csv" %i
        ##########
        ##### cp to current DIR
        #######
        mmyfile="""apass_TWO_MASS_%d.csv""" %i
        os.system('ifdh cp -D %s .' %myfile)
       
        if  not os.path.exists("./"+mmyfile):
            print "file was not copyed try to link it \n" 
#        os.symlink(myfile1,mmyfile)

#        mmyfile="""apass_TWO_MASS_%d.csv""" %i
        df= pd.read_csv(mmyfile)   
        good_data.append(df)

    chunk = pd.concat(good_data, ignore_index=True).sort(['RAJ2000_APASS'], ascending=True) 
    w1 = ( chunk['RAJ2000_2MASS'] > minra ) ; w2 = ( chunk['RAJ2000_2MASS'] < maxra )
    w3 = ( chunk['DEJ2000_2MASS'] > mindec ); w4 = ( chunk['DEJ2000_2MASS'] < maxdec )
    w5 = ( chunk[BANDname] >0 )
    
    datastd = chunk[ w1 &  w2 & w3 & w4 & w5 ]
    datastd1= pd.DataFrame({'MATCHID':datastd['MATCHID'],'RA':datastd['RAJ2000_2MASS'],'DEC':datastd['DEJ2000_2MASS'],'WAVG_MAG_PSF':datastd[BANDname]})

    col=["MATCHID", "RA","DEC", "WAVG_MAG_PSF"]

    datastd1.to_csv(outfile,columns=col,sep=',',index=False)

    hdr=["MATCHID","RA","DEC","wavg_mag_psf"]

    for i in range(data['RA_CENT'].size):
        stdlistFile ="""%s_std.csv"""   % (data['FILENAME'][i])    
        minra=min(data['RA_CENT'][i],data['RAC1'][i],data['RAC2'][i],data['RAC3'][i],data['RAC4'][i])-.1
        maxra=max(data['RA_CENT'][i],data['RAC1'][i],data['RAC2'][i],data['RAC3'][i],data['RAC4'][i])+.1
        mindec=min(data['DEC_CENT'][i],data['DECC1'][i],data['DECC2'][i],data['DECC3'][i],data['DECC4'][i])-.1
        maxdec=max(data['DEC_CENT'][i],data['DECC1'][i],data['DECC2'][i],data['DECC3'][i],data['DECC4'][i])+.1
        w1 = ( datastd1['RA'] > minra ) ;w2 = ( datastd1['RA'] < maxra )
        w3 = ( datastd1['DEC'] > mindec );w4 = ( datastd1['DEC'] < maxdec )
        df = datastd1[ w1 &  w2 & w3 & w4 ].sort(['RA'], ascending=True)
        df.to_csv(stdlistFile,columns=col,sep=',',index=False)

##################################
#New plots ONLY if NEEDED!!
def plotradec_sexvsY2Q1(args):
    import numpy as np
    import string
    import sys
    import matplotlib.pyplot as plt

    if args.verbose >0 : print args

    catlistFile="""D%08d_r%sp%1d_red_catlist.csv""" % (args.expnum,args.reqnum,args.attnum)

    if not os.path.isfile(catlistFile):
        print '%s does not seem to exist... exiting now...' % catlistFile
        sys.exit(1)

    data=np.genfromtxt(catlistFile,dtype=None,delimiter=',',names=True)
    correctccd = (data['CCDNUM'] == args.ccd)
    data = data[ correctccd ]

    for i in range(data['FILENAME'].size):
        ra1=[];ra2=[];dec1=[];dec2=[]
        catFilename = os.path.basename(data['FILENAME'][i])
        rac  = data['RA_CENT'][i] ;     decc = data['DEC_CENT'][i]
        rac1 = data['RAC1'][i] ;        decc1= data['DECC1'][i]
        rac2 = data['RAC2'][i] ;        decc2= data['DECC2'][i]
        rac3 = data['RAC3'][i] ;        decc3= data['DECC3'][i]
        rac4 = data['RAC4'][i] ;        decc4= data['DECC4'][i]
        CCDpoints=[[rac2,decc2],[rac,decc2],[rac3,decc3],[rac3,decc],[rac4,decc4],[rac,decc4],[rac1,decc1],[rac1,decc]]
        ccdline = plt.Polygon(CCDpoints,  fill=None, edgecolor='g')

        pnglistout="""%s.png""" % (catFilename)
        objlistFile="""%s_Obj.csv""" % (catFilename)
        stdlistFile="""%s_std.csv""" % (catFilename)        

        if not os.path.isfile(objlistFile):
            print '%s does not seem to exist... exiting now...' % objlistFile
            sys.exit(1)

        if not os.path.isfile(stdlistFile):
            print '%s does not seem to exist... exiting now...' % stdlistFile
            sys.exit(1)

        # Read in the file...
        ra1=np.genfromtxt(stdlistFile,dtype=float,delimiter=',',skiprows=1,usecols=(1))
        dec1=np.genfromtxt(stdlistFile,dtype=float,delimiter=',',skiprows=1,usecols=(2))
        ra2=np.genfromtxt(objlistFile,dtype=float,delimiter=',',skiprows=1,usecols=(1))
        dec2=np.genfromtxt(objlistFile,dtype=float,delimiter=',',skiprows=1,usecols=(2))
        plt.axes()
        plt.gca().add_patch(ccdline)
        plt.scatter(ra1,dec1,marker='.')
        plt.scatter(ra2,dec2,c='r', marker='+')
        line = plt.Polygon(CCDpoints,  fill=None, edgecolor='r')

        plt.title(catFilename, color='#afeeee') 
        plt.savefig(pnglistout, format="png" )

        ra1=[];ra2=[];dec1=[];dec2=[]
        
        plt.clf() 

##################################
#Matching FILES MAKE SURE the Cols are still in the same order
#
def matchSortedStdwithObsCats(inputStdFile,inputObsFile,outputMatch,racolStdFile,deccolStdFile,racolObsFile,deccolObsFile,matchArcsec,verbose):

    import math
    import sys
    
    f1=inputStdFile
    f2=inputObsFile
    outfile=outputMatch
    stdracol=racolStdFile
    stddeccol=deccolStdFile
    obsracol=racolObsFile
    obsdeccol=deccolObsFile
    matchTolArcsec=matchArcsec
    verbose=verbose

    #print f1,f2,outfile,stdracol,stddeccol,obsracol,obsdeccol,matchTolArcsec,verbose

    # Initialize "dictionaries"...
    # Each element of a "dictionary" is associated with a standard star.
    # Each element is a list of information from the potential matches from the
    #  observed data.
    raDict=[]
    decDict=[]
    obslineDict=[]
    
    # Initialize lists of standard stars...
    # These are actually lists of standards within a given sliding window of RA.
    stdra_win=[]
    stddec_win=[]
    stdline_win=[]
    
    # Open the output file for the standard star/observed star matches...
    ofd=open(outfile,'w')

    # Initialize running match id
    matchid=0
    
    # Open the standard star CSV file...
    fd1=open(f1)
    
    # Read header line of standard star CSV file...
    h1=fd1.readline()
    h1n=h1.strip().split(',')
    
    # Open CSV file of observed data...
    fd2=open(f2)
    
    # Read header line of observed data CSV file...
    h2=fd2.readline()
    h2n=h2.strip().split(',')
    obsracol=h2n.index("RA")

    # Create and output header for the output CSV file...
    #  Note that the column names from the standard star file
    #  now have a suffix of "_1", and that column names from
    #  the observed star file now have a suffix of "_2".
    outputHeader='MATCHID'
    for colhead in h1n:
        outputHeader=outputHeader+','+colhead.upper()+'_1'
    for colhead in h2n:
        outputHeader=outputHeader+','+colhead.upper()+'_2'
    outputHeader=outputHeader+'\n'
    ofd.write(outputHeader)
    

    # initialize some variables
    #  done_std = "are we done reading the standard stars file?"
    #  done_obs = "are we done reading the observations file?"
    #  stdra, stddec = "current values for standard star RA,DEC"
    #  obsra, obsdec = "current values for observed star RA,DEC"
    #  tol = sky angular separation tolerance (in degrees)
    #  tol2 = square of tol
    #  tolrawin = half-range of RA window (in degrees)
    #  linecnt = "line count"
    done_std=0
    done_obs=0
    stdra=-999
    stddec=-999
    obsra=-999
    obsdec=-999
    tol=matchTolArcsec/3600.0
    tol2=tol*tol
    tolrawin=3.*tol

    linecnt=0
    
    # Loop through file of observed data...
    while (done_obs == 0):

        # Increment line count from observed data file...
        linecnt += 1
        if ( (linecnt/1000.0 == int(linecnt/1000.0)) and (verbose > 1) ):
            print '\r'+'Progress (lines read from observed catalog):  ',linecnt,
            sys.stdout.flush()

        # Read line from observed data file...
        l2=fd2.readline()

        # Are we done reading through the file of observed data yet?
        # If so, set done_obs=1 and ignore the rest of the loop;
        # otherwise, process the data line and continue with the
        # rest of the loop...
        if l2 == "":
            done_obs = 1
            continue
        else:
            #obsline2 holds the whole line of information for this entry for
            # future use...
            obsline2=l2.strip()
            l2s=l2.strip().split(',')
            obsra=float(l2s[obsracol])
            obsdec=float(l2s[obsdeccol])
        #endif


        # Update the sliding RA window of standard stars...
        #  ... but only if stdra-obsra <= tolrawin, 
        #  ... and only if we haven't previously finished
        #      reading the standard star file...
        while ( (stdra-obsra <= tolrawin) and (done_std == 0) ):

            # Read the next line from the standard star file...
            l1=fd1.readline()
        
            # if we have reached the end of the standard star file,
            # set done_std=1 and skip the rest of this code block; 
            # otherwise, process the new line...
            if l1 ==  "":

                done_std=1

            else:

                l1s=l1.strip().split(',')
                stdra_new=float(l1s[stdracol])
                stddec_new=float(l1s[stddeccol])

                # if the new standard star RA (stdra_new) is at or above the 
                #  lower bound, add this standard star to the sliding RA 
                #  window...
                if ((stdra_new-obsra) >= -tolrawin):

                    # update values of stdra, stddec...
                    stdra = stdra_new
                    stddec = stddec_new

                    # add the standard star info to lists of ra, dec, and general
                    #  data for this sliding window...
                    stdra_win.append(stdra)
                    stddec_win.append(stddec)
                    stdline_win.append(l1.strip())
            
                    # initialize lists for possible observed star/standard star 
                    #  matches and add these (still empty) lists to "dictionaries" 
                    #  associated with this sliding window of standard stars...
                    raDict.append([])
                    decDict.append([])
                    obslineDict.append([])
            
                #endif
            
            #endif

        #endwhile -- inner "while" loop

        
        # Find the first good match (not necessarily the best match) between this
        # observed star and the set of standard stars within the sliding RA
        # window... 
        # (We might want to revisit this choice -- i.e., of first match vs. best
        #  match -- in the future.)
        
        cosd=math.cos(math.radians(obsdec))

        # Loop through all standards stars i in the sliding RA window for that
        #  observed star...
        for i in range(0,len(stdra_win)):

            delta2=(obsra-stdra_win[i])*(obsra-stdra_win[i])*cosd*cosd+(obsdec-stddec_win[i])*(obsdec-stddec_win[i])

            # Is the sky position of standard star i (in the sliding RA window)
            #  within the given radial tolerance of the observed star?  If so, 
            #  add the observed info to that standard star's dictionaries...
            if float(delta2) < float(tol2):
                raDict[i].append(obsra)
                decDict[i].append(obsdec)
                obslineDict[i].append(obsline2)
                # if we found one match, we take it and break out of this "for"
                #  loop...
                break
            #endif 

        #endfor


        # Do some cleanup of the lists and "dictionaries" associated with the 
        #  sliding RA window and output matches to output file...
        #  For each iteration of this while loop, we look at the "zeroth"
        #  standard star in the sliding RA window and remove it if it
        #  turns out to be now outside the RA tolerance window.
        while ( (len(stdra_win) > 1) and (obsra-stdra_win[0] > tolrawin) ):

            # Loop through all the observations matched with this standard 
            # star...
            # (Note that many standard stars may have zero matches...)
            for j in range(0,len(raDict[0])):

                # increment the running star id
                matchid += 1
                        
                # output line to the match file...
                outputLine = """%d,%s,%s\n""" % (matchid,stdline_win[0],obslineDict[0][j])
                ofd.write(outputLine)

            #endfor

            # Delete the dictionaries associated with this standard star
            #  (star "0" in the sliding RA window)...
            del raDict[0]
            del decDict[0]
            del obslineDict[0]
    
            # Delete the lists associated with standard star
            #  (star "0" in the sliding RA window)...
            del stdra_win[0]
            del stddec_win[0]
            del stdline_win[0]

        #endwhile -- inner "while" loop

    #endwhile -- outer "while" loop
        

    # Do some cleanup of the lists and "dictionaries" associated with the sliding 
    #  RA window after reading last line of observed data file and output matches
    #  to output file...
    while (len(stdra_win) > 0):

        # Loop through all the observations matched with this standard star...
        # (Note that many standard stars may have zero matches...)
        for j in range(0,len(raDict[0])):

            # increment the running star id
            matchid += 1

            # output line to the match file...
            outputLine = """%d,%s,%s\n""" % (matchid,stdline_win[0],obslineDict[0][j])
            ofd.write(outputLine)

        #endfor

        # Delete the dictionaries associated with this standard star
        #  (star "0" in the sliding RA window)...
        del raDict[0]
        del decDict[0]
        del obslineDict[0]
    
        # Delete the lists associated with standard star
        #  (star "0" in the sliding RA window)...
        del stdra_win[0]
        del stddec_win[0]
        del stdline_win[0]
            
    #endwhile
        
    # close the input and output files...
    fd1.close()
    fd2.close()
    ofd.close()

    return 0

##################################
# Get 3sigma clipped Zero point and iterater
##################################

def sigmaClipZP(args):
    import astropy 
    from astropy.stats import sigma_clip
    import numpy as np
    from numpy import mean
    import numpy.ma as ma 
    import string
    import sys
    import math

    if args.verbose >0 : print args

    catlistFile="""D%08d_r%sp%1d_red_catlist.csv""" % (args.expnum,args.reqnum,args.attnum)
    if not os.path.isfile(catlistFile):
        print '%s does not seem to exist... exiting now...' % catlistFile
        sys.exit(1)

    data1=np.genfromtxt(catlistFile,dtype=None,delimiter=',',names=True)
    correctccd = (data1['CCDNUM'] == args.ccd)
    data1 = data1[ correctccd ]
    ZeroListFile="""Zero_D%08d_%02d_r%sp%1d.csv""" % (args.expnum,args.ccd,args.reqnum,args.attnum)
    
    fout=open(ZeroListFile,'w')
    hdr = "FILENAME,Nall,Nclipped,ZP,ZPrms,magType\n"
    fout.write(hdr)

    for i in range(data1['FILENAME'].size):
        data1name = data1['FILENAME']
        if data1['FILENAME'].size>1:
            data1name = data1['FILENAME'][i]
        data1name = data1name.astype(str)
        # problem with filetocheck being a numpy array
        if isinstance(data1name, np.ndarray):
            data1name = data1name[0]
#        data1name = np.array2string(data1name).strip('\'') # problem with filetocheck being a numpy array
#        data1name = data1name[data1name.find('\'')+1:] # hacky fix in case of unicode encoding
#        data1name = data1name[:-2] # hacky fix in case of unicode encoding

        catFilename = os.path.basename(data1name)
        matchListFile="%s_match.csv" % (catFilename)        

        if not os.path.isfile(matchListFile):
            print '%s does not seem to exist... exiting now...' % matchListFile
            sys.exit(1)

        try:
            #add new cuts for Apass9-2mass data set
            data11=np.genfromtxt(matchListFile,dtype=None,delimiter=',',names=True)
            #New CUTS
            w0=(data11['MAG_2']-data11['WAVG_MAG_PSF_1']-25.0) < -10
            w1=(data11['MAG_2']-data11['WAVG_MAG_PSF_1']-25.0) > -40
            data=data11[ w0 & w1 ]

            # Identify band...      It is now BAND_2!!
            bandList = data['BAND_2'].tolist()
            band = bandList[0].upper()

            WAVG_MAG_PSF_1       =data['WAVG_MAG_PSF_1']
            MAG_2                =data['MAG_2']
            delt_mag_data        =MAG_2 -WAVG_MAG_PSF_1 -25.0
            filtered_data        =sigma_clip(delt_mag_data, 3, 3, np.mean, copy=True)
            NumStarsClipped      =(filtered_data).count()
            NumStarsAll          =len(filtered_data)

            if  NumStarsClipped >2:
                sigclipZP=np.mean(filtered_data)
                stdsigclipzp=np.std(filtered_data)/math.sqrt(NumStarsClipped)

            else:
                sigclipZP=-999
                stdsigclipzp=-999
        except:
            sigclipZP      =-999
            stdsigclipzp   =-999
            NumStarsClipped=0
            NumStarsAll    =0

        line = """%s,%d,%d,%f,%f,%s""" % (catFilename, NumStarsAll,NumStarsClipped,sigclipZP,stdsigclipzp,args.magType)

        fout.write(line+'\n')

#Added new!
    fout.close()        

    ZeroListFile="""Zero_D%08d_%02d_r%sp%1d.csv""" % (args.expnum,args.ccd,args.reqnum,args.attnum)
    if not os.path.isfile(catlistFile):
        print '%s does not seem to exist... exiting now...' % ZeroListFile
        sys.exit(1)

    MergedFile="""Merged_D%08d_%02d_r%sp%1d.csv""" % (args.expnum,args.ccd,args.reqnum,args.attnum)
    jointwocsv(catlistFile,ZeroListFile,MergedFile)

##################################
#Given two csv join both output to MergedFile
def jointwocsv(file1,file2,MergedFile):    
    import csv
    from collections import OrderedDict

    filenames = file1, file2
    data = OrderedDict()
    fieldnames = []
    for filename in filenames:
        with open(filename, "rb") as fp: 
            reader = csv.DictReader(fp)
            fieldnames.extend(reader.fieldnames)
            for row in reader:
                data.setdefault(row["FILENAME"], {}).update(row)

    fieldnames = list(OrderedDict.fromkeys(fieldnames))
    with open(MergedFile, "wb") as fp:
        writer = csv.writer(fp)
        writer.writerow(fieldnames)
        for row in data.itervalues():
            writer.writerow([row.get(field, '') for field in fieldnames])
#
#
##################################
# Modified at 10/09/2017 
# changed from 357 to 350 
#
# round ra 360.0--0.0
def roundra(ra):
#    if ra < 357:
    if ra < 356:
        return ra
    else:
        return ra-360.

##################################
# Get 3sigma clipped Zero point and iterater
##################################

def sigmaClipZPallCCDs(args):
    import pandas as pd
    import numpy as np
    import sys,os,glob,math,string
    import astropy 
    from astropy.stats import sigma_clip
    from numpy import mean
    import numpy.ma as ma 

    if args.verbose >0 : print args
    #allZPout="""allZP_D%08d_r%sp%1d.csv""" % (args.expnum,args.reqnum,args.attnum)	
    allZPout="""allZP_D%08d_r%sp%1d.csv""" % (args.expnum,args.reqnum,args.attnum)
    stdfile="""STDD%08d_r%sp%1d_red_catlist.csv""" % (args.expnum,args.reqnum,args.attnum)
    objfile="""ObjD%08d_r%sp%1d_red_catlist.csv""" % (args.expnum,args.reqnum,args.attnum)
    outfile="""OUTD%08d_r%sp%1d_red_catlist.csv""" % (args.expnum,args.reqnum,args.attnum)
    stddf = pd.read_csv(stdfile).sort(['RA'], ascending=True)
    #read  file and sort and save 
    stddf.to_csv(stdfile,sep=',',index=False)
    path='./'
    all_files = glob.glob(os.path.join(path, "*Obj.csv")) # make sure the columns are still in the same order TODO
    df = pd.concat((pd.read_csv(f) for f in all_files))
    #df = df.sort(['RA'], ascending=True) 
    df = df.sort(['RA'], ascending=True) # guessed to sort by number... originally by RA
    #TODO: oscillates between no RA field and having one????
    # fields are EXPNUM  CCDNUM  NUMBER  ALPHAWIN_J2000  DELTAWIN_J2000     FLUX_AUTO FLUXERR_AUTO      FLUX_PSF  FLUXERR_PSF   MAG_AUTO SPREADERR_MODEL  FWHM_WORLD  FWHMPSF_IMAGE  FWHMPSF_WORLD  CLASS_STAR FLAGS  IMAFLAGS_ISO  ZeroPoint  ZeroPoint_rms  ZeroPoint_FLAGS

    #read all file and sort and save 
    df.to_csv(objfile,sep=',',index=False)

    stdracol=1; stddeccol=2
    #obsracol=1 ; obsdeccol=2 
    obsracol=df.columns.get_loc("RA") ; obsdeccol=df.columns.get_loc("DEC")
    matchTolArcsec=1.0 #1.0arcsec 
    verbose=2
    matchSortedStdwithObsCats(stdfile,objfile,outfile,stdracol,stddeccol,obsracol,obsdeccol,matchTolArcsec,verbose)

    if not os.path.isfile(outfile):
	print '%s does not seem to exist... exiting now...' % outfile
	sys.exit(1)
    try:
	data11=np.genfromtxt(outfile,dtype=None,delimiter=',',names=True)
        #New CUTS
	w0=(data11['MAG_2']-data11['WAVG_MAG_PSF_1']-25.0) < -10
	w1=(data11['MAG_2']-data11['WAVG_MAG_PSF_1']-25.0) > -40
	data=data11[ w0 & w1 ]

        # Identify band...      It is now BAND_2!!
	bandList = data['BAND_2'].tolist()
	band = bandList[0].upper()

	WAVG_MAG_PSF_1       =data['WAVG_MAG_PSF_1']
	MAG_2                =data['MAG_2']
	delt_mag_data        =MAG_2 -WAVG_MAG_PSF_1 -25.0
	filtered_data        =sigma_clip(delt_mag_data, 3, 3, np.mean, copy=True)
	NumStarsClipped      =(filtered_data).count()
	NumStarsAll          =len(filtered_data)

	if  NumStarsClipped >2:
		sigclipZP=np.mean(filtered_data)
		stdsigclipzp=np.std(filtered_data)/math.sqrt(NumStarsClipped)
	else:
		sigclipZP=-999
		stdsigclipzp=-999
    except:
	sigclipZP      =-999
	stdsigclipzp   =-999
	NumStarsClipped=0
	NumStarsAll    =0

    hdr="EXPNUM,REQNUM,ATTNUM,NumStarsAll,NumStarsClipped,sigclipZP,stdsigclipzp\n"    
    line = """%d,%s,%d,%d,%d,%f,%f""" % (args.expnum,args.reqnum,args.attnum, NumStarsAll,NumStarsClipped,sigclipZP,stdsigclipzp)
    fout=open(allZPout,'w')
    fout.write(hdr)
    fout.write(line+'\n')

###################################
# Detect OUTLIERS- and update files
# NEEDS WORK--
# NEED to ADD LOF-see JoinZP3
###################################
def ZP_OUTLIERS(args):
    import pandas as pd
    import numpy as np
    import os,sys,glob
    import math
    import sklearn
    from sklearn.neighbors import NearestNeighbors
    import matplotlib.pyplot as plt
    import matplotlib as mpl


    if args.verbose >0 : print args
    #allZeroFile="""allZP_D%08d_r%sp%1d.csv""" % (args.expnum,args.reqnum,args.attnum)	
    allZeroFile="""allZP_D%08d_r%sp%1d.csv""" % (args.expnum,args.reqnum,args.attnum)
    #if not os.path.isfile(catlistFile):
    #    print '%s does not seem to exist... exiting now...' % ZeroListFile
    #    sys.exit(1)

    MergedFile="""Merged_D%08d_%02d_r%sp%1d.csv""" % (args.expnum,args.ccd,args.reqnum,args.attnum)
    if not os.path.isfile(MergedFile):
        print '%s does not seem to exist... exiting now...' % MergedFile
        sys.exit(1)

    fout="""Merg_allZP_D%08d_%02d_r%sp%1d.csv""" % (args.expnum,args.ccd,args.reqnum,args.attnum)

    df1=pd.read_csv(MergedFile)
    df2=np.genfromtxt(allZeroFile,dtype=None,delimiter=',',names=True)

    w0= ( df1['Nclipped'] < 4 ) | ( df1['ZP'] < -100 ) | ( df1['ZPrms'] > 0.3 )
    df1['NewZP']=np.where(w0 , df2['sigclipZP'],df1['ZP'])
    df1['NewZPrms']=np.where(w0, df2['stdsigclipzp'],df1['ZPrms'])
    df1['NewZPFlag1']=np.where(w0, np.int16(1),np.int16(0))
                    
    df1['DiffZP']=df1['NewZP']- df1.NewZP.median()

    #Currently Diff ZP=0.3mag That is TOO MUCH    
    w2= ( abs(df1['DiffZP']) < 0.15 )
    df1['NewZPFlag2']=np.where(w2 ,np.int16(0),np.int16(-1000))        
    df1['Percent1']=100.0*np.count_nonzero(df1['NewZPFlag2'])/len(df1['NewZP'])

    #if 20% CCD (i.e 12 CCDs out 60)
    w3= ( df1['Percent1'] >= 20 )  
    df1['NewZPFlag3']=np.where(w3 ,np.int16(-9999),np.int16(0))

    df1['NewZPFlag']=df1['NewZPFlag1']+df1['NewZPFlag2']+df1['NewZPFlag3']
    
    df1['DiffZP1']=1000.0*df1['DiffZP']                                            

    df1.to_csv(MergedFile,sep=',',index=False)

#That will not work around 0-360
#New
    w= ( df1['RA_CENT'] >150 ) 
    df1['roundRA_CENT']=np.where(w, 360.-df1['RA_CENT'], df1['RA_CENT'])
 
    #As still listed in *_immask.fits Fits heareder
    #PIXSCAL1=0.27 / [arcsec/pixel] Pixel scale, axis1
    #PIXSCAL1=0.27 / [arcsec/pixel] Pixel scale, axis2
    df1['x'] = (1./0.27)*3600*(df1['roundRA_CENT']-df1['roundRA_CENT'].median())*math.cos(math.radians(df1['DEC_CENT'].median()))                                                                                
    df1['y'] = (1./0.27)*3600*(df1['DEC_CENT'] - df1['DEC_CENT'].median())

    cols=['FILENAME','EXPNUM','CCDNUM','NewZP','NewZPrms','NewZPFlag']
    df1.to_csv(fout,columns=cols,sep=',',index=False)
    
    #########################################################
    ######### Extra ONLY if the Full Exposure have problem!
    #########################################################
    #FIND OUTLIER via Local Outlier Factor(LOF) see
    #Ref:
    #  http://www.dbs.ifi.lmu.de/Publikationen/Papers/LOF.pdf
    #  THIS is a TEST
    #########################################################
    #try
    #mask = ( df1['NewZPFlag3'] < 0 )
    #if ( df1[mask]['x'].size > 0 ):
        #data = df1[['x','y','DiffZP1']]            
        ##You can change below value for different MinPts
        ##m=5,10,15,30,35,40,45,50,55 testing
        #m=10

        #knndist, knnindices = knn(data,3)
        #reachdist, reachindices = reachDist(data,m,knndist)
        
        #irdMatrix = lrd(m,reachdist)
        #lofScores = lof(irdMatrix,m,reachindices) 
        #scores= pd.DataFrame(lofScores,columns=['Score'])
        #mergedData=pd.merge(data,scores,left_index=True,right_index=True)
        #mergedData['flag'] = mergedData.apply(returnFlag,axis=1)
        #Outliers = mergedData[(mergedData['flag']==1)]
        #Normals = mergedData[(mergedData['flag']==0)]

        #mergedData1=pd.concat([df1, mergedData], axis=1) 
        #mergedData1.to_csv(fout,sep=',',index=False)

        #print Outliers
        
        #pnglistout0="""%s_ZP_Warning.png""" % (catlistFile)
        #pnglistout1="""%s_ZP_Score.png""" % (catlistFile)
        ################
        # New plot in X,Y
        # NEED to convert the RA, DEC vs the expCal Zero-point mag 
        # New plot SCORE - histogram 
        ################


        #l1=plt.scatter(Normals['x'],Normals['y'],Normals['DiffZP1'],c='b',marker='o')
        #l2=plt.scatter(Outliers['x'],Outliers['y'],Outliers['DiffZP1'],c='r',marker='*')
        #plt.legend((l1,l2),('Regular','Outlier'),scatterpoints=1,loc='upper left',ncol=1, fontsize=9)
        #plt.title('Warning D%08d_r%sp%1d %s-Band' %(args.expnum,args.reqnum,args.attnum,BAND))   
        #plt.xlabel(r"$X$", size=18)
        #plt.ylabel(r"$Y$", size=18)
        #plt.xlim([min(data['x']),max(data['x'])])
        #plt.ylim([min(data['y']),max(data['y'])])
        #plt.savefig(pnglistout0, format="png" )
        #plt.clf() 

        #SCORE - histogram
        #plt.hist(mergedData['Score'],bins=100,facecolor='red')
        #plt.xlabel('LOF Score')
        #plt.ylabel('Frequency')
        #plt.title('Outlier Scores')
        #plt.savefig(pnglistout1, format="png" )
        #plt.clf() 

        
##################################            
#New plots for Zero-Points
def plotradec_ZP(args):
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib as mpl
    import sys

    if args.verbose >0 : print args

    catlistFile="""D%08d_r%sp%1d_red_catlist.csv""" % (args.expnum,args.reqnum,args.attnum)
    if not os.path.isfile(catlistFile):
        print '%s does not seem to exist... exiting now...' % catlistFile
        sys.exit(1)

    #ZeroListFile="""Zero_D%08d_r%sp%1d.csv""" % (args.expnum,args.reqnum,args.attnum)
    #if not os.path.isfile(catlistFile):
    #    print '%s does not seem to exist... exiting now...' % ZeroListFile
    #    sys.exit(1)        
    #Mergedout="""Merged_D%08d_r%sp%1d.csv""" % (args.expnum,args.reqnum,args.attnum)    
    #print catlistFile,ZeroListFile,Mergedout
    #jointwocsv(catlistFile,ZeroListFile,Mergedout)
    #MergedFile="""Merg_allZP_D%08d_r%sp%1d.csv""" % (args.expnum,args.reqnum,args.attnum)

    MergedFile="""Merged_D%08d_%02d_r%sp%1d.csv""" % (args.expnum,args.ccd,args.reqnum,args.attnum)    
    data=np.genfromtxt(MergedFile,dtype=None,delimiter=',',names=True)
    stdRA = np.std(data['RA_CENT'])
    if stdRA > 20:
        data['RA_CENT']=[roundra(x) for x in data['RA_CENT']]
        data['RAC1']   =[roundra(x) for x in data['RAC1']]
        data['RAC2']   =[roundra(x) for x in data['RAC2']]
        data['RAC3']   =[roundra(x) for x in data['RAC3']]
        data['RAC4']   =[roundra(x) for x in data['RAC4']]
    
    #    print " Unexpected value of the RA spred stdRA=%f \n" % stdRA
    #    sys.exit(1)
    w0=(data['ZP']==-999)
    w1=(data['ZP']>-999)

    data0=data[w0]
    data1=data[w1]

    if (len(data1)==0):
         sys.exit(1)

    BAND=data1['BAND'][0]
    zpmedian=np.median(data1['ZP'])

    pnglistout0="""%s_ZP.png""" % (catlistFile)
    pnglistout1="""%s_deltaZP.png""" % (catlistFile)
    pnglistout2="""%s_NumClipstar.png""" % (catlistFile)
    pnglistout3="""%s_CCDsvsZPs.png""" % (catlistFile)

    w2=( data['NewZPFlag'] ==0 )
    w3=( data['NewZPFlag'] ==1 )
    
    #w3=(data('NewZPFlag') >0 )
    data2=data[w2]
    data3=data[w3]    
    pnglistout4="""%s_NewZP.png""" % (catlistFile)
    pnglistout5="""%s_NewdeltaZP.png""" % (catlistFile)
    
    minra=min(min(data['RA_CENT']),min(data['RAC1']),min(data['RAC2']),min(data['RAC3']),min(data['RAC4']))-.075
    mindec=min(min(data['DEC_CENT']),min(data['DECC1']),min(data['DECC2']),min(data['DECC3']),min(data['DECC4']))-.075
    maxra=max(max(data['RA_CENT']),max(data['RAC1']),max(data['RAC2']),max(data['RAC3']),max(data['RAC4']))+.075
    maxdec=max(max(data['DEC_CENT']),max(data['DECC1']),max(data['DECC2']),max(data['DECC3']),max(data['DECC4']))+.075

    ################
    #New plot the RA, DEC vs the expCal Zero-point mag 
    ################
    l1=plt.scatter(data0['RA_CENT'], data0['DEC_CENT'], c=data0['ZP'], s=15, marker=(25,0), cmap=mpl.cm.spectral,vmin=np.min(data1['ZP']), vmax=np.max(data1['ZP']))
    l2=plt.scatter(data1['RA_CENT'], data1['DEC_CENT'], c=data1['ZP'], s=500, marker=(5,0), cmap=mpl.cm.spectral,vmin=np.min(data1['ZP']), vmax=np.max(data1['ZP']))
    cbar=plt.colorbar(ticks=np.linspace(np.min(data1['ZP']),np.max(data1['ZP']), 4))    
    cbar.set_label('Zero-Point Mag')
    #plt.legend((l1,l2),('No Y2Q1 data','ExpCal'),scatterpoints=1,loc='upper left',ncol=1, fontsize=9)
    plt.legend((l1,l2),('No APASS data','ExpCal'),scatterpoints=1,loc='upper left',ncol=1, fontsize=9)

    for i in range(data['RA_CENT'].size):
        CCDpoints=[[data['RAC2'][i],data['DECC2'][i]],[data['RAC3'][i],data['DECC3'][i]],[data['RAC4'][i],data['DECC4'][i]],[data['RAC1'][i],data['DECC1'][i]]]
        ccdline = plt.Polygon(CCDpoints,  fill=None, edgecolor='k')
        plt.gca().add_patch(ccdline)

    plt.title('D%08d_r%sp%1d %s-Band ZP_Median=%.3f ' %(args.expnum,args.reqnum,args.attnum,BAND,zpmedian))   
    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(pnglistout0, format="png" )
    plt.clf() 

    ################
    #New plot the RA, DEC vs the expCal Delta Zero-point mag from median 
    ################
    
    l1=plt.scatter(data0['RA_CENT'], data0['DEC_CENT'], c=data0['ZP'], s=15, marker=(25,0), cmap=mpl.cm.spectral,vmin=np.min(data1['ZP']), vmax=np.max(data1['ZP']))
    l2=plt.scatter(data1['RA_CENT'], data1['DEC_CENT'], c=1000*(data1['ZP']-zpmedian), s=500, marker=(5,0), cmap=mpl.cm.spectral,vmin=min(1000*(data1['ZP']-zpmedian)), vmax=max(1000*(data1['ZP']-zpmedian)))
    cbar=plt.colorbar(ticks=np.linspace(min(1000*(data1['ZP']-zpmedian)),max(1000*(data1['ZP']-zpmedian)), 4))    
    cbar.set_label('Delta Zero-Point mili-Mag')
    plt.legend((l1,l2),('No APASS data','ExpCal'),scatterpoints=1,loc='upper left',ncol=1, fontsize=9)

    for i in range(data['RA_CENT'].size):
        CCDpoints=[[data['RAC2'][i],data['DECC2'][i]],[data['RAC3'][i],data['DECC3'][i]],[data['RAC4'][i],data['DECC4'][i]],[data['RAC1'][i],data['DECC1'][i]]]
        ccdline = plt.Polygon(CCDpoints,  fill=None, edgecolor='k')
        plt.gca().add_patch(ccdline)

    plt.title('D%08d_r%sp%1d %s-Band ZP_Median=%.3f ' %(args.expnum,args.reqnum,args.attnum,BAND,zpmedian))   

    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(pnglistout1, format="png" )
    plt.clf() 

    ################
    #New plot RA DEC vs Number of stars clipped stars from expCal 
    ################
    l1=plt.scatter(data0['RA_CENT'], data0['DEC_CENT'], c=data0['Nclipped'], s=15, marker=(25,0), cmap=mpl.cm.spectral)
    l2=plt.scatter(data1['RA_CENT'], data1['DEC_CENT'], c=data1['Nclipped'], s=500, marker=(5,0), cmap=mpl.cm.spectral)

    cbar=plt.colorbar()
    cbar.set_label('No. 3 $\sigma$ clipped Stars')
    plt.legend((l1,l2),('No APASS data','expCal'),scatterpoints=1,loc='upper left',ncol=1, fontsize=9)

    for i in range(data['RA_CENT'].size):
        CCDpoints=[[data['RAC2'][i],data['DECC2'][i]],[data['RAC3'][i],data['DECC3'][i]],[data['RAC4'][i],data['DECC4'][i]],[data['RAC1'][i],data['DECC1'][i]]]
        ccdline = plt.Polygon(CCDpoints,  fill=None, edgecolor='k')
        plt.gca().add_patch(ccdline)
   
    plt.title('D%08d_r%sp%1d %s-Band ZP_Median=%.3f ' %(args.expnum,args.reqnum,args.attnum,BAND,zpmedian))
   
    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(pnglistout2, format="png" )
    plt.clf() 

################    
#New plot CCDs vs ZP from expCal
    plt.errorbar(data0['CCDNUM'], data0['ZP'], data0['ZPrms'], fmt='o',label='No APASS data')
    plt.errorbar(data1['CCDNUM'], data1['ZP'], data1['ZPrms'], fmt='o',label='expCal')
    legend = plt.legend(loc='upper center', shadow=None, fontsize=12)
    legend.get_frame().set_facecolor('#00FFCC')
    plt.title('D%08d_r%sp%1d %s-Band ZP_Median=%.3f ' %(args.expnum,args.reqnum,args.attnum,BAND,zpmedian))
    plt.xlabel(r"$CCDs$", size=18)
    plt.ylabel(r"$Zero Points$", size=18)
    plt.ylim(min(data1['ZP'])-.01,max(data1['ZP'])+.02)
    plt.xlim(min(data1['CCDNUM'])-1.5,max(data1['CCDNUM'])+1.5)
    plt.savefig(pnglistout3, format="png" )
    plt.clf()

    ################
    #New plot the RA, DEC vs the NEW Zero-point mag 
    ################
    l1=plt.scatter(data2['RA_CENT'], data2['DEC_CENT'], c=data2['NewZP'], s=500, marker=(5,0), cmap=mpl.cm.spectral,vmin=np.min(data2['NewZP']), vmax=np.max(data2['NewZP']))
    l2=plt.scatter(data3['RA_CENT'], data3['DEC_CENT'], c=data3['NewZP'], s=25, marker=(25,0), cmap=mpl.cm.spectral,vmin=np.min(data2['NewZP']), vmax=np.max(data2['NewZP']))
    cbar=plt.colorbar(ticks=np.linspace(np.min(data2['NewZP']),np.max(data2['NewZP']), 4))    
    cbar.set_label('Zero-Point Mag')
    #CHANGE
    plt.legend((l1,l2),('CCD','allEXP'),scatterpoints=1,loc='upper left',ncol=1, fontsize=9)
    for i in range(data['RA_CENT'].size):
        CCDpoints=[[data['RAC2'][i],data['DECC2'][i]],[data['RAC3'][i],data['DECC3'][i]],[data['RAC4'][i],data['DECC4'][i]],[data['RAC1'][i],data['DECC1'][i]]]
        ccdline = plt.Polygon(CCDpoints,  fill=None, edgecolor='k')
        plt.gca().add_patch(ccdline)

    plt.title('D%08d_r%sp%1d %s-Band ' %(args.expnum,args.reqnum,args.attnum,BAND))   
    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(pnglistout4, format="png" )
    plt.clf() 

    ################
    #New plot the RA, DEC vs the NEW Delta Zero-point mag from median 
    ################
    l1=plt.scatter(data2['RA_CENT'], data2['DEC_CENT'], c=data2['DiffZP1'], s=500, marker=(5,0), cmap=mpl.cm.spectral,vmin=np.min(data2['DiffZP1']), vmax=np.max(data2['DiffZP1']))   
    l2=plt.scatter(data3['RA_CENT'], data3['DEC_CENT'], c=data3['DiffZP1'], s=25, marker=(25,0), cmap=mpl.cm.spectral,vmin=min(data2['DiffZP1']), vmax=max(data2['DiffZP1']))
    cbar=plt.colorbar(ticks=np.linspace(min(data2['DiffZP1']),max(data2['DiffZP1']), 4))    
    cbar.set_label('Delta Zero-Point mili-Mag')
    plt.legend((l1,l2),('CDD','allExP'),scatterpoints=1,loc='upper left',ncol=1, fontsize=9)

    for i in range(data['RA_CENT'].size):
        CCDpoints=[[data['RAC2'][i],data['DECC2'][i]],[data['RAC3'][i],data['DECC3'][i]],[data['RAC4'][i],data['DECC4'][i]],[data['RAC1'][i],data['DECC1'][i]]]
        ccdline = plt.Polygon(CCDpoints, fill=None, edgecolor='k')
        plt.gca().add_patch(ccdline)

    plt.title('D%08d_r%sp%1d %s-Band ' %(args.expnum,args.reqnum,args.attnum,BAND))   

    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(pnglistout5, format="png" )
    plt.clf() 
#
#
##################################
# Read SEX_table filemane_fullcat.fits then 
# apply_ZP with FLAGS
# and write ONE file for all CCDs as
#    filemane_fullcat.fits_Obj.csv
#

def apply_ZP_Sexcatalogfitstocsv(catFilename,EXPNUM,CCDNUM,zeropoint,zeropoint_rms,ZPFLAG):
    import fitsio
    import string,math,csv
    import numpy as np

    outFile="""%s_Obj.csv""" % (catFilename)    
    extension=2
    
    col=['EXPNUM','CCDNUM','NUMBER','ALPHAWIN_J2000','DELTAWIN_J2000','FLUX_AUTO','FLUXERR_AUTO','FLUX_PSF','FLUXERR_PSF','MAG_AUTO','MAGERR_AUTO','MAG_PSF','MAGERR_PSF','SPREAD_MODEL','SPREADERR_MODEL','FWHM_WORLD','FWHMPSF_IMAGE','FWHMPSF_WORLD','CLASS_STAR','FLAGS','IMAFLAGS_ISO','ZeroPoint','ZeroPoint_rms','ZeroPoint_FLAGS']

    hdr=['NUMBER','ALPHAWIN_J2000','DELTAWIN_J2000','FLUX_AUTO','FLUXERR_AUTO','FLUX_PSF','FLUXERR_PSF','MAG_AUTO','MAGERR_AUTO','MAG_PSF','MAGERR_PSF','SPREAD_MODEL','SPREADERR_MODEL','FWHM_WORLD','FWHMPSF_IMAGE','FWHMPSF_WORLD','CLASS_STAR','FLAGS','IMAFLAGS_ISO']

    catFilename = str(catFilename.astype(str))
#    catFilename = np.array2string(catFilename).strip('\'') # problem with filetocheck being a numpy array
#    catFilename = catFilename[catFilename.find('\'')+1:] # hacky fix in case of unicode encoding

    data = fitsio.read(catFilename,  columns=hdr, ext=extension)[:]
    data = data[np.argsort(data['ALPHAWIN_J2000'])]

    w1=( data['FLUX_AUTO'] >0. )
    data['MAG_AUTO'] = np.where(w1 , (-2.5*np.log10(data['FLUX_AUTO']) - zeropoint) ,np.int16(-9999))
    data['MAGERR_AUTO'] = np.where(w1 ,(2.5/math.log(10.))*(data['FLUXERR_AUTO']/data['FLUX_AUTO']) ,np.int16(-9999))
    w1=( data['FLUX_PSF'] >0. )
    data['MAG_PSF']= np.where(w1 , (-2.5*np.log10(data['FLUX_PSF']) - zeropoint )   ,np.int16(-9999)) 
    data['MAGERR_PSF'] =  np.where(w1 ,(2.5/math.log(10.))*(data['FLUXERR_PSF']/data['FLUX_PSF']) ,np.int16(-9999))

    with open(outFile,'w') as csvFile:
            writer = csv.writer(csvFile,delimiter=',',  quotechar='|',
                                lineterminator='\n', quoting=csv.QUOTE_MINIMAL)

            writer.writerow(col)

            line=[]
            for i in range(data.size):
                line=EXPNUM,CCDNUM,data['NUMBER'][i],data['ALPHAWIN_J2000'][i],data['DELTAWIN_J2000'][i],data['FLUX_AUTO'][i],data['FLUXERR_AUTO'][i],data['FLUX_PSF'][i],data['FLUXERR_PSF'][i],data['MAG_AUTO'][i],data['MAGERR_AUTO'][i],data['MAG_PSF'][i],data['MAGERR_PSF'][i],data['SPREAD_MODEL'][i],data['SPREADERR_MODEL'][i],data['FWHM_WORLD'][i],data['FWHMPSF_IMAGE'][i],data['FWHMPSF_WORLD'][i],data['CLASS_STAR'][i],data['FLAGS'][i],data['IMAFLAGS_ISO'][i],zeropoint,zeropoint_rms,ZPFLAG

                writer.writerow(line)

    data=[]

#############################################
#To Do
#Still needs new args for type of output csv/fits
def Onefile(args):
    import os,sys,glob
    import csv
    import pandas as pd
    from astropy.io import fits

    if args.verbose >0 : print args

    catlistFile="""Merg_allZP_D%08d_%02d_r%sp%1d.csv""" % (args.expnum,args.ccd,args.reqnum,args.attnum)
    if not os.path.isfile(catlistFile):
        print '%s does not seem to exist...' % catlistFile

    fout="""D%08d_%02d_r%sp%1d_ZP.csv""" % (args.expnum,args.ccd,args.reqnum,args.attnum)
    fitsout="""D%08d_%02d_r%sp%1d_ZP.fits""" % (args.expnum,args.ccd,args.reqnum,args.attnum)

    #Removed Feb23,2017
    #os.system('rm %s ' %fitsout)

    data=np.genfromtxt(catlistFile,dtype=None,delimiter=',',names=True)
        
    for i in range(data['FILENAME'].size):
        dataname = data['FILENAME']
        dataexpnum = data['EXPNUM'] 
        dataccdnum = data['CCDNUM'] 
        datazp = data['NewZP'] 
        datazprms = data['NewZPrms'] 
        datazpflag = data['NewZPFlag'] 
        if data['FILENAME'].size>1:
            dataname = data['FILENAME'][i]
            dataexpnum = data['EXPNUM'][i]
            dataccdnum = data['CCDNUM'][i]
            datazp = data['NewZP'][i]
            datazprms = data['NewZPrms'][i]
            datazpflag = data['NewZPFlag'][i]
        apply_ZP_Sexcatalogfitstocsv(dataname,dataexpnum,dataccdnum,datazp,datazprms,datazpflag)
        
    path='./'
    all_files = glob.glob(os.path.join(path, "*Obj.csv"))     

    big_frame=pd.concat((pd.read_csv(f) for f in all_files)).sort(['ALPHAWIN_J2000'], ascending=True)

    big_frame['ID']=list(range(len(big_frame['ALPHAWIN_J2000'].index)))
    big_frame['ID']=1+big_frame['ID']
    
    Cols=["ID","EXPNUM","CCDNUM","NUMBER","ALPHAWIN_J2000","DELTAWIN_J2000","FLUX_AUTO","FLUXERR_AUTO","FLUX_PSF","FLUXERR_PSF","MAG_AUTO","MAGERR_AUTO","MAG_PSF","MAGERR_PSF","SPREAD_MODEL","SPREADERR_MODEL","FWHM_WORLD","FWHMPSF_IMAGE","FWHMPSF_WORLD","CLASS_STAR","FLAGS","IMAFLAGS_ISO","ZeroPoint","ZeroPoint_rms","ZeroPoint_FLAGS"]

    big_frame.to_csv(fout,sep=',',columns=Cols,index=False)

#Later Please ADD new args for args.fits/args.csv  with if one/or and
#Currently BOTH csv and fits are written to disk with  NO ARGS!
    
    col1   = fits.Column(name='ID',format='J', array=np.array(big_frame['ID']))
    col2   = fits.Column(name='EXPNUM',format='I',array=np.array(big_frame['EXPNUM']))
    col3   = fits.Column(name='CCDNUM',format='I', array=np.array(big_frame['CCDNUM']))
    col4   = fits.Column(name='NUMBER',format='I', array=np.array(big_frame['NUMBER']))
    col5   = fits.Column(name='ALPHAWIN_J2000',format='D',array=np.array(big_frame['ALPHAWIN_J2000']))
    col6   = fits.Column(name='DELTAWIN_J2000',format='D', array=np.array(big_frame['DELTAWIN_J2000']))
    col7   = fits.Column(name='FLUX_AUTO',format='D', array=np.array(big_frame['FLUX_AUTO']))
    col8   = fits.Column(name='FLUXERR_AUTO',format='D', array=np.array(big_frame['FLUXERR_AUTO']))
    col9   = fits.Column(name='FLUX_PSF',format='D', array=np.array(big_frame['FLUX_PSF']))
    col10  = fits.Column(name='FLUXERR_PSF',format='D', array=np.array(big_frame['FLUXERR_PSF']))
    col11  = fits.Column(name='MAG_AUTO',format='D', array=np.array(big_frame['MAG_AUTO']))
    col12  = fits.Column(name='MAGERR_AUTO',format='D',array=np.array(big_frame['MAGERR_AUTO'])) 
    col13  = fits.Column(name='MAG_PSF',format='D', array=np.array(big_frame['MAG_PSF']))
    col14  = fits.Column(name='MAGERR_PSF',format='D', array=np.array(big_frame['MAGERR_PSF']))
    col15  = fits.Column(name='SPREAD_MODEL',format='D',array=np.array(big_frame['SPREAD_MODEL']))
    col16  = fits.Column(name='SPREADERR_MODEL',format='D',array=np.array(big_frame['SPREADERR_MODEL']))
    col17  = fits.Column(name='FWHM_WORLD',format='D', array=np.array(big_frame['FWHM_WORLD']))
    col18  = fits.Column(name='FWHMPSF_IMAGE',format='D',array=np.array(big_frame['FWHMPSF_IMAGE'])) 
    col19  = fits.Column(name='FWHMPSF_WORLD',format='D',array=np.array(big_frame['FWHMPSF_WORLD'])) 
    col20  = fits.Column(name='CLASS_STAR',format='D', array=np.array(big_frame['CLASS_STAR']))
    col21  = fits.Column(name='FLAGS',format='I',array=np.array(big_frame['FLAGS'])) 
    col22  = fits.Column(name='IMAFLAGS_ISO',format='I', array=np.array(big_frame['IMAFLAGS_ISO']))
    col23  = fits.Column(name='ZeroPoint',format='D', array=np.array(big_frame['ZeroPoint']))
    col24  = fits.Column(name='ZeroPoint_rms',format='D', array=np.array(big_frame['ZeroPoint_rms']))
    col25  = fits.Column(name='ZeroPoint_FLAGS',format='I', array=np.array(big_frame['ZeroPoint_FLAGS']))

    cols=fits.ColDefs([col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24,col25])

    tbhdu=fits.BinTableHDU.from_columns(cols)
    # clobber flag writes out even if the output file already existed (deprecated, replaced by overwrite)
    tbhdu.writeto(fitsout,clobber=True)

##################################
#
#
####################################
#knn function gets the dataset and calculates K-Nearest neighbors and distances
def knn(df,k):
    from sklearn.neighbors import NearestNeighbors
    
    nbrs = NearestNeighbors(n_neighbors=3)
    nbrs.fit(df)
    distances, indices = nbrs.kneighbors(df)
    return distances, indices

##################
#reachDist calculates the reach distance of each point to MinPts around it
def reachDist(df,MinPts,knnDist):
    from sklearn.neighbors import NearestNeighbors
    import numpy as np
    
    nbrs = NearestNeighbors(n_neighbors=MinPts)
    nbrs.fit(df)
    distancesMinPts, indicesMinPts = nbrs.kneighbors(df)
    distancesMinPts[:,0] = np.amax(distancesMinPts,axis=1)
    distancesMinPts[:,1] = np.amax(distancesMinPts,axis=1)
    distancesMinPts[:,2] = np.amax(distancesMinPts,axis=1)
    return distancesMinPts, indicesMinPts

##################
#lrd calculates the Local Reachability Density
def lrd(MinPts,knnDistMinPts):
    import numpy as np
    return (MinPts/np.sum(knnDistMinPts,axis=1))

##################
#Finally lof calculates lot outlier scores
def lof(Ird,MinPts,dsts):
    lof=[]
    for item in dsts:
       tempIrd = np.divide(Ird[item[1:]],Ird[item[0]])
       lof.append(tempIrd.sum()/MinPts)
    return lof

##################
#We flag anything with outlier score greater than 1.2 as outlier
#This is just for charting purposes
def returnFlag(x):
    if x['Score']>1.2:
       return 1
    else:
       return 0
    
##################################

if __name__ == "__main__":
    main()

##################################
