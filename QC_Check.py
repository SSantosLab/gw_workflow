#! /usr/bin/env python
"""
Perfoms an assessent of exposures from a first/final cut run.  The quality 
of the exposures is based upon the seeing (FWHM), background, and extinction
due to clouds.



     
"""
#import argparse
import os
#import stat
import time
import timeit
import math
#import re
import csv
import sys
#import datetime
import glob
import subprocess
import numpy as np
import fitsio
import pandas as pd
#from setuptools.command.easy_install import sys_executable
import string
from setuptools.command.easy_install import sys_executable
#from pandas.io.pytables import timeit
#from curses.has_key import system
class QC_Check:
    def __init__(self):
#        self.expnum = expnum
#        self.nite = nite
#        self.band = band
        self.catfile = ''
        self.base_mjd = {65241.0:[56240.0,56267.6667],56268.0:[56267.6667,56419.6667],56520.0:[56519.6667,56631.6667],
                56632.0:[56631.6667,56811.6667],56876.0:[56875.6667,56974.6667],56975.0:[56974.6667,57105.6667],
                57235.0:[57234.6667,57288.6667],57289.0:[57288.6667,57372.6667],57373.0:[57372.6667,57442.6667]}
        self.DougD = {65241.0:{'g':[26.8605,-0.0008,-0.1808,0.0211],'r':[26.9148,-0.0010,-0.0876,0.0241],'i':[26.8786,-0.0017,-0.0829,0.0211],
                      'z':[26.5817,-0.0050,-0.0539,0.0303],'Y':[25.4886,-0.0041,-0.0827,0.0320],'u':[25.2969,-0.0002,-0.5254,0.0766]},
             56268.0:{'g':[26.9457,-0.0017,-0.1718,0.0287],'r':[27.0220,-0.0015-0.1000,0.0299],'i':[26.9366,-0.0013,-0.0691,0.0326],
                      'z':[26.6734-0.0020,-0.0968,0.0615],'Y':[25.4959,-0.0016,-0.0529,0.0564],'u':[25.2969,-0.0002,-0.5254,0.0766]},
             56520.0:{'g':[26.8289,-0.0001,-0.1906,0.0307],'r':[26.9701,-0.0005,-0.0939,0.0245],'i':[26.9192,-0.0005,-0.0676,0.0225],
                      'z':[26.6800,-0.0008,-0.0971,0.0482],'Y':[25.4818,-0.0004,-0.0630,0.0463],'u':[25.2969,-0.0002,-0.5254,0.0766]},
             56632.0:{'g':[26.8731,-0.0016,-0.1445,0.0243],'r':[26.9542,-0.0012,-0.0766,0.0215],'i':[26.8759,-0.0010,-0.0534,0.0234],
                     'z':[26.5745,-0.0012,-0.0648,0.0504],'Y':[25.3895,-0.0012,-0.0294,0.0536],'u':[25.2969,-0.0002,-0.5254,0.0766]},
             56876.0:{'g':[26.8445,-0.0005,-0.1761,0.0262],'r':[26.9227,-0.0004,-0.0991,0.0275],'i':[26.8750,-0.0003,-0.0758,0.0273],
                      'z':[26.5552,-0.0002,-0.0654,0.0467],'Y':[25.4231,-0.0003,-0.0531,0.0339],'u':[25.2969,-0.0002,-0.5254,0.0766]},
             56975.0:{'g':[26.8276,-0.0009,-0.1594,0.0299],'r':[26.8936,-0.0006,-0.0766,0.0293],'i':[26.8371,-0.0007,-0.0519,0.0270],
                      'z':[26.5515,-0.0007,-0.0704,0.0465],'Y':[25.3991,-0.0005,-0.0375,0.-376],'u':[25.2969,-0.0002,-0.5254,0.0766]},
             57235.0:{'g':[26.7487,0.0002,-0.1842,0.0272],'r':[26.8518,0.0002,-0.1031,0.0261],'i':[26.8061,-0.00001,-0.0718,0.0228],
                      'z':[26.5261,0.0002,-0.0815,0.0444],'Y':[25.4113,-0.0013,-0.0545,0.0344],'u':[25.2969,-0.0002,-0.5254,0.0766]},
             57289.0:{'g':[26.7450,0.0002,-0.1898,0.0254],'r':[26.8227,0.0001,-0.0882,0.0318],'i':[26.7818,0.00001,-0.0587,0.0234],
                      'z':[26.4665,0.0002,-0.0516,0.0415],'Y':[25.3502,0.0005,-0.0618,0.0360],'u':[25.2969,-0.0002,-0.5254,0.0766]},
             57373.0:{'g':[26.7809,-0.0009,-0.1750,0.0219],'r':[26.8765,-0.0007,-0.0964,0.0227],'i':[26.8148,-0.0008,-0.0651,0.0247],
                      'z':[26.5038,-0.0010,-0.0666,0.0475],'Y':[25.3920,-0.0010,-0.-739,0.0436],'u':[25.2969,-0.0002,-0.5254,0.0766]}}
        
        self.ki={'u': 0.436,'g': 0.192,'r':0.097,'i':0.071,'z':0.083,'Y':0.067}
#
    def write_assoc_pandas(self, assoc_file,names=['FILEPATH_LOCAL','BAND','MAG_ZERO'],sep=' '):
        self.logger.info("Writing CCDS files information to: %s" % assoc_file)
       
        variables = [self.ctx.assoc[name] for name in names]
        df = pd.DataFrame(zip(*variables), columns=names)
        df.to_csv(assoc_file,index=False,sep=sep)
        return

    @staticmethod
    def write_dict2pandas(mydict, fileN, names=['FILEPATH_LOCAL','BAND','EXPNUM'],sep=' ', logger=None):
        variables = [mydict[name] for name in names]
        df = pd.DataFrame(zip(*variables), columns=names)
        df.to_csv(fileN,index=False,sep=sep)
        return
    
    def getPSF_fwhm(self,fitsfile):
        psf_fwhm = 0.0

        fits1 = fitsio.FITS(fitsfile,'r')
        psfhdr = fits1[1].read_header()

        try:
            psf_fwhm = float(psfhdr['PSF_FWHM'])
        except:
            print "PSF_FWHM not fount \n"
        return psf_fwhm
    
    def getSkybrite(self,fitsfile):
        skyb = 0.
        sky_brite = 0.
        mjd_obs = 0.
        airmass = 1.3
        gaina=1.
        gainb=1.
        exptime=90.
        compress = False
        if string.find(fitsfile,'.fz') > 0:
            compress = True
        fits1 = fitsio.FITS(fitsfile,'r')
        imhdr = fits1[0].read_header()
        ZD = 0.
        if compress:
            imhdr = fits1[1].read_header()
        try:
            sky_brite = float(imhdr['SKYBRITE'])
            gaina = float(imhdr['GAINA'])
            gainb = float(imhdr['GAINB'])
            ZD = float(imhdr['ZD'])
        except:
            print "SKYBRITE not fount \n"
        try:
            exptime = float(imhdr['EXPTIME'])
        except:
            exptime=90.
            print "EXPTIME not found \n"
        try:
            mjd_obs = float(imhdr['MJD-OBS'])
        except:
            print " MJD-OBS not found \n"
        try:
            airmass = float(imhdr['AIRMASS'])
        except:
#            print " AIRMASS not found set default value 1.3\n"
            airmass = 1./np.cos(np.radians(ZD))
        if (exptime>0.01):
            efactor=exptime
        else:
            efactor=1.0
#        print " gaina=%f gainb=%f \n" % (gaina,gainb)
        gtesta=gaina-1.
        gtestb=gainb-1.
        if ((abs(gtesta)<0.5)and(abs(gtestb)<0.5)):
#               The case where gains are 1... therefore units are electrons
            gfactor=4.0
#                ccd_info[ccdnum]['bunit']='e-'
                
        else:
#               The case where gains are not 1... therefore units are already in counts
            gfactor=1.0
#                ccd_info[ccdnum]['bunit']='DN'

        skyb=sky_brite/efactor/gfactor


#        skyb = sky_brite/(exptime*(gaina+gainb)/2.)
        return (skyb,mjd_obs,airmass,exptime)
 
    def getNobj(self,fitsfile):
        nobj = 0.
        compress = False
        if string.find(fitsfile,'.fz') > 0:
            compress = True
        fits1 = fitsio.FITS(fitsfile,'r')
        hdr = fits1[2].read_header()
        if compress:
            hdr = fits1[3].read_header()
        try:
            nobj = float(hdr['NAXIS2'])
 
        except:
            print "NAXIS2 not fount \n"

        return nobj
     
    def getBaseMJD(self,mjd):
        
        bmjd = 65241.0
        for base in self.base_mjd:
            mmin = self.base_mjd[base][0]
            mmax = self.base_mjd[base][1]
            if mjd >= mmin and mjd <= mmax:
                bmjd = base
                break
            bmjd = base
        return bmjd
     
    def getDmagPar(self,bmjd,band):

        bandpar = self.DougD[bmjd]
        print bandpar
        a_0 = bandpar[band][0]
        a_1 = (self.DougD[bmjd])[band][1]
        k =  (self.DougD[bmjd])[band][2]
        return [a_0,a_1,k]  
    
    def scamp(self):
        dx=0.
        dy = 0.
        chi2 = 0
        nstars=0
        start = timeit.default_timer()
        inputFile='*fullcat.fits'
        self.base_dir = '/data/des51.a/data/kuropat/BLISS/'
        file_list = glob.glob(inputFile)
        sort_list = sorted(file_list)  
        nccd = len(sort_list)
        line = sort_list[0]
        self.prefix= string.split(line,'_')[0]
        self.band = string.split(line,'_')[1]
        print nccd
        if nccd > 60:
            self.head_File = self.base_dir+self.band+'no61.head'
        else:
            self.head_File = self.base_dir+self.band+'no2no61.head'
        print self.head_File
        self.default_scamp = self.base_dir+'default2.scamp.20140423'

        self.outputFile = self.prefix+'allcat.fits' 
        if os.path.exists(self.outputFile):
            os.remove(self.outputFile)
        cmd = ['fitscombine']

        for fileN in sort_list:
            cmd.append(fileN)
        cmd.append(self.outputFile)

        retval = subprocess.call(cmd)
        print retval
        self.catalog_ref = 'GAIA-DR1'
        cmd = 'scamp ' + self.outputFile +\
        ' ' + '-AHEADER_GLOBAL ' + self.head_File +\
        ' -ASTRINSTRU_KEY DUMMY -AHEADER_SUFFIX .aheadnoexist -ASTREFMAG_LIMITS -99,17 ' +\
        ' -ASTREF_CATALOG ' +self.catalog_ref +' -c ' +self.default_scamp +\
        ' -WRITE_XML Y -XML_NAME scamp.xml -MOSAIC_TYPE SAME_CRVAL -ASTREF_BAND DEFAULT -POSITION_MAXERR 10.0 -NTHREADS 1' +\
        ' -REF_SERVER cocat1.u-strasbg.fr,vizier.nao.ac.jp,vizier.cfa.harvard.edu '

        print '\n',cmd,'\n'
        retval=''
        try:
            retval = subprocess.check_output(cmd.split(),stderr=subprocess.STDOUT)
        except ValueError as err:
            print(err.args)
            sys.exit(1)
        lines = string.split(retval,'\n')
        n=0
        nlines = len(lines)
        for i in range(nlines):
            line = lines[i]
#            print line
            if string.find(line,'Astrometric stats (external):') > 0:
                n+=1
                tokens = string.split(lines[i+4])
                dx = float(string.split(tokens[6],'"')[0])
                dy = float(string.split(tokens[7],'"')[0])
                chi2 = float(tokens[8])
                nstars = int(tokens[9])
#                print tokens
                break
        scamptime = timeit.default_timer() - start
        print "Scamp time= %f \n" % scamptime
        return (dx,dy,chi2,nstars)
    
    def unpackLog(self,logFile):
        dx = 0.
        dy = 0.
        chi2 = 0
        nstars = 0
        lines=[]
        for line in open(logFile,'r'):
            lines.append(line)
        nlines = len(lines)
        for i in range(nlines):
            line = lines[i]
            if string.find(line,'Astrometric stats (external):') > 0:
                tokens = string.split(lines[i+4])
                dx = float(string.split(tokens[6],'"')[0])
                dy = float(string.split(tokens[7],'"')[0])
                chi2 = float(tokens[8])
                nstars = int(tokens[9])
#                print tokens
                break
        return (dx,dy,chi2,nstars)
        
if __name__ == "__main__":
    qc_start = timeit.default_timer()
    print sys.argv
    expnum = sys.argv[1]
    nite = sys.argv[2]
    bandIn = sys.argv[3]
    sys.stdout.flush()
#    ftxt = open('qc_test.txt','w')
    fcsv = open("qc_test.csv" ,'w')
    writer = csv.writer(fcsv, delimiter=',',  quotechar='"', quoting=csv.QUOTE_MINIMAL)
    out_row=[]
    out_row.append("expid")
    out_row.append("dm_accept")
    out_row.append("scamp_decide")
    out_row.append("dax1")
    out_row.append("dax2")
    out_row.append("chi2")
    out_row.append("nstar")
    out_row.append("teff_decide")
    out_row.append("teff_f")
    out_row.append("teff_b")
    out_row.append("teff_c")
    out_row.append("teff")
    out_row.append("psf_fwhm")
    out_row.append("skybrite")
    out_row.append("n_objects")
    writer.writerow(out_row)

    qct = QC_Check()
    print "expnum=%s nite=%s band=%s \n" % (expnum,nite,bandIn)


    pi=3.141592654
    halfpi=pi/2.0
    deg2rad=pi/180.0
#
#   Define pixel size 
#
    pixsize=0.263
    fp_rad=1.2
#   Below (fwhm_DMtoQC_offset was an empirical offset when using FWHM_WORLD)
    fwhm_DMtoQC_offset_world=1.10
#   New veresion (an additive offset needed when comparing FWHM_MEAN (from PSFex) with respect to QC)
    fwhm_DMtoQC_offset_psfex=+0.04
    
    magerr_thresh=0.1
    magnum_thresh=20
    magbin_min=10.
    magbin_max=25.
    magbin_step=0.25
    mbin=np.arange(magbin_min,magbin_max,magbin_step)
    band2i={"u":0,"g":1,"r":2,"i":3,"z":4,"Y":5,"VR":6}
#
#
    kolmogorov={"u":1.2,"g":1.103,"r":1.041,"i":1.00,"z":0.965,"Y":0.95,"VR":1.04}
    teff_lim={  "u":0.2,"g":0.2,  "r":0.3,  "i":0.3, "z":0.3,  "Y":0.2,"VR":0.3}
    seeing_lim={}
    seeing_fid={}
#
#   Set seeing cutoff to be 1.6 times Kolmogov except at "Y" which should
#   be forced to match that at g-band
#
    for bandi in ["u","g","r","i","z","Y","VR"]:
        if (bandi == "Y"):
            seeing_lim[bandi]=1.6*kolmogorov["g"]
        else:
            seeing_lim[bandi]=1.6*kolmogorov[bandi]
#       Commented version below was needed when using FWHM_WORLD
#        seeing_fid[band]=fwhm_DMtoQC_offset*0.9*kolmogorov[band]
#       Now fiducial value is additive (and applied to the FWHM_MEAN value coming from PSFex)
##        seeing_fid[bandi]=0.9*kolmogorov[bandi]
        seeing_fid[bandi]=kolmogorov[bandi]

#   Surface brightness limits from Eric Nielson which were derived "...from a few 
#   exposures from a photometric night in SV with little moon (20121215)"
#
    sbrite_good={"u":0.2,"g":1.05,"r":2.66,"i":7.87,"z":16.51,"Y":14.56,"VR":3.71}
    sbrite_lim={"u":0.8,"g":4.0,"r":9.58,"i":21.9,"z":50.2,"Y":27.6,"VR":13.58}
#
#   These (the above) were originally based on the following estimate by Annis
#   sbrite_good={"u":2.0,"g":1.2,"r":3.8,"i":8.7,"z":20.0,"Y":11.0}
#   roughly equivalent to grizY=22.09,21.21,20.12,18.95,18.00 mag/sq-arcsec
#
#   APASS and NOMAD magnitude limits
#
    glimit=90.0
    rlimit=90.0
    ilimit=90.0
#
    jlimit=16.0
    blimit=18.0
#
#   APASS and NOMAD convergence criteria (stop performing cross_correltions when
#   percentage of last 300 attempts are below the limit
#
    a100_lim=1
    n100_lim=3
#



    csvfiles = glob.glob('allZP*.csv')
    if len(csvfiles) > 0:
        statsinfo = os.stat(csvfiles[0])
        if statsinfo.st_size <= 0:
            sys.exit(-1)
    else:
        sys.exit(-1)
#    print csvfiles
    d = pd.read_csv(csvfiles[0])
#    print d
    exp_rec = {}
    exp_rec['expnum'] = expnum
    exp_rec['nite'] = nite
    exp_rec['band'] = bandIn

    exp_rec['zp'] = str(d['sigclipZP'][0])
    exp_rec['zprms'] = str(d['stdsigclipzp'][0])
    """  First lets check results of the scamp """
    logFile = str(expnum)+'.log'
    start = timeit.default_timer()
    if os.path.exists(logFile):
        " extract scamp results from log file "
        (dx,dy,chi2,ns) = qct.unpackLog(logFile)
    else:
        (dx,dy,chi2,ns) = qct.scamp()
    print (dx,dy,chi2,ns) 
    logtime = timeit.default_timer() - start
    print " Log analysis time= %f \n" % logtime
#    print exp_rec
    start=timeit.default_timer()
    allPsf = glob.glob('*.psf')
    psf_width = []
    for psfFile in allPsf:
        psf_width.append(qct.getPSF_fwhm(psfFile))
    psf_fwhm = np.median(np.array(psf_width))
#    print ' PSF_FWHM=%f n ' % psf_fwhm
    exp_rec['psfex_fwhm'] = psf_fwhm*pixsize 
    psftime = timeit.default_timer() - start
    print "PSF extraction time=%f \n" % psftime
    skybritness = []
    mjd_obs = []
    airmass = []
    exptimes = []
    start=timeit.default_timer()
    allImages = glob.glob('*_immask.fits')
    if len(allImages) == 0:
        allImages = glob.glob('*_immask.fits.fz')
    for imFile in allImages:
        mytup = qct.getSkybrite(imFile)
        skybritness.append(mytup[0])
        mjd_obs.append(mytup[1])
        airmass.append(mytup[2])
        exptimes.append(mytup[3])
        
    
 
    skyb =  np.median(np.array(skybritness))
    MJD = np.median(np.array(mjd_obs))
    airm =  np.median(np.array(airmass))
    exptime = np.median(np.array(exptimes))
    imagetime =  timeit.default_timer() - start
    print " Image proc time %f \n" % imagetime
    detObj = []
    start=timeit.default_timer()
    allCat = glob.glob('*_fullcat.fits')
    if len(allCat) == 0:
        allCat = glob.glob('*_fullcat.fits.fz')
    for catFile in allCat:
        nobj = qct.getNobj(catFile)
        detObj.append(nobj)   
    allObj =  np.sum(np.array(detObj))
    exp_rec['N_OBJ'] = allObj
    exp_rec['skyb_avg'] = skyb
    cattime =  timeit.default_timer() - start
    print "Catalog proc time %f \n" % cattime
    start=timeit.default_timer()   
    mjd0 = qct.getBaseMJD(MJD)
    print ' MJD=%f bmjd=%f \n' %(MJD,mjd0)
    fpars = qct.getDmagPar(mjd0,bandIn)
    a_0 = fpars[0]
    a_1 = fpars[1]
    k = fpars[2]
    dmag = a_0 + a_1*(MJD - mjd0) + k*airm
#    zp_eff = -2.5*math.log10(exptime) - float(exp_rec['zp']) + qct.ki[exp_rec['band']]*airm
    zp_eff = -2.5*math.log10(exptime) - float(exp_rec['zp']) - qct.ki[exp_rec['band']]*airm
#    print 'Comp mag zp_eff = %f \n' % zp_eff
#    exp_rec["magdiff"] = zp_eff - dmag
    exp_rec["magdiff"] = dmag -zp_eff
#    print ' dmag=%f magdiff=%f \n' % (dmag,exp_rec["magdiff"])
###############################################################################
#   Now the calculations for the Teff (and of course the individual components)
#
#   Calculate F_eff
#   Note code is now updated to use the psfex_fwhm (with fwhm_world used as a fallback)
#
    use_fwhm=-1.0
#   Uncomment the following line if you want to force runs to use FWHM_WORLD (i.e. for tests)
#    exp_rec['psfex_fwhm']=-1.0

#    use_fwhm=exp_rec['psfex_fwhm']+fwhm_DMtoQC_offset_psfex
    use_fwhm=exp_rec['psfex_fwhm']
#    print 'use_fwhm=%f \n' % use_fwhm
#
#   OK so I lied above... calculate F_eff (NOW!)
#
    if (use_fwhm > 0.0):
#        print 'seeing_fid = %f \n' % seeing_fid[exp_rec["band"]]
        exp_rec["teff_f"]=(seeing_fid[exp_rec["band"]]*seeing_fid[exp_rec["band"]]/(use_fwhm*use_fwhm))
    else:
        print("# WARNING:  No FWHM measure available. F_EFF set to -1.0")
        exp_rec["teff_f"]=-1.0
#
#   Calculate B_eff
#
    if (exp_rec["skyb_avg"]>0.0):
        exp_rec["teff_b"]=sbrite_good[exp_rec["band"]]/exp_rec["skyb_avg"]
    else:
        print("# WARNING:  No SKY BRIGHTNESS measure available. B_EFF set to -1.0")
        exp_rec["teff_b"]=-1.0
#
#   Calculate C_eff
#
    
    if ((float(exp_rec["magdiff"])>-95.) and (float(exp_rec["magdiff"])<95.0)):
        if float(exp_rec["magdiff"])<0.2:
            exp_rec["teff_c"]=1.0
        else:
            exp_rec["teff_c"]=math.pow(10.0,(-2.0*(float(exp_rec["magdiff"])-0.2)/2.5))
    else:
        print("# WARNING:  No CLOUD measure available. C_EFF set to -1.0")
        exp_rec["teff_c"]=-1.0

#
#   Calculate T_eff
#
    value_teff=1.0
    if (exp_rec["teff_f"]>=0):
        value_teff=value_teff*exp_rec["teff_f"]
    if (exp_rec["teff_b"]>=0):
        value_teff=value_teff*exp_rec["teff_b"]
    if (exp_rec["teff_c"]>=0):
        value_teff=value_teff*exp_rec["teff_c"]
    if ((exp_rec["teff_f"]<0)or(exp_rec["teff_b"]<0)):
        exp_rec["teff"]=-1.
    else:
        exp_rec["teff"]=value_teff
    t7=time.time()
    calctime=timeit.default_timer() - start
    print " Teff calculation time=%f \n" % calctime
    print("# ")
    print("# ")
    print("#              FWHM Summary   ")
    print("#------------------------------------")
    print("#        TEFF_F = {:7.3f} ".format(exp_rec['teff_f']))
    print("#        TEFF_B = {:7.3f} ".format(exp_rec['teff_b']))
    print("#        TEFF_C = {:7.3f} ".format(exp_rec['teff_c']))
    print("#        TEFF = {:7.3f} ".format(exp_rec['teff']))
    print("#     PSFex(FWHM_MEAN) = {:7.3f} ".format(exp_rec['psfex_fwhm']))
    print("# ")

###############################################################################
#   Everything is now ready to make an assessment and to output it to the 
#   proper locations.
#
    scamp_decide ="none"
    new_decide="none"
    if ((exp_rec["teff"]<0.)or(exp_rec["teff_c"]<0.0)):
        new_decide="unkn"
    elif (exp_rec["teff"]>teff_lim[exp_rec["band"]]):
        new_decide="good"
#        if (exp_rec["fwhm_world"]>seeing_lim[exp_rec["band"]]):
        if (use_fwhm>seeing_lim[exp_rec["band"]]):
            new_decide="badF"
    else:
        new_decide="badT"
#
    
    if dx <= 0.1 and dy <= 0.1:
        scamp_decide="good"
    else:
        scamp_decide="bad"
    dm_process="True"
    if (new_decide == "good" and scamp_decide == "good"):
        dm_accept="True"
    elif (new_decide == "unkn"):
        dm_accept="Unknown"
    else:
        dm_accept="False"
        


    out_row=[]
    out_row.append(exp_rec["expnum"])
    out_row.append(dm_accept)
    out_row.append(scamp_decide)
    out_row.append(dx)
    out_row.append(dy)
    out_row.append(chi2)
    out_row.append(ns)
    out_row.append(new_decide)
    out_row.append(exp_rec["teff_f"])
    out_row.append(exp_rec["teff_b"])
    out_row.append(exp_rec["teff_c"])
    out_row.append(exp_rec["teff"])
    out_row.append(exp_rec["psfex_fwhm"])
    out_row.append(skyb)
    out_row.append(exp_rec["N_OBJ"])
    writer.writerow(out_row)  
    qctime = timeit.default_timer() - qc_start
    print " total qc_time=%f \n" % qctime
