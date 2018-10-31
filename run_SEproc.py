from desdmLiby1e2 import * 
import numpy as np
import os
import glob
from joblib import Parallel, delayed


#for ccd in CCD:
#    ccdstring= "%02d"%int(ccd)
#    pixcorrect('detrend',ccdstring,**args)
#    nullweight('detrend', 'nullweight', ccdstring,**args)
#    sextractor('nullweight', 'sextractor', ccdstring, **args)
#    psfex( 'sextractor', ccdstring, **args)
def run_stage_1(ccd):
    ccdstring= "%02d"%int(ccd)
    pixcorrect('detrend',ccdstring,**args)
    nullweight('detrend', 'nullweight', ccdstring,**args)
    sextractor('nullweight', 'sextractor', ccdstring, **args)

#run "stage 2" loop over all ccds
#for ccd in CCD:
#    ccdstring= "%02d"%int(ccd)
#    bleedmask(ccdstring,'wcs','bleedmasked',**args)
#    skycompress(ccdstring,'bleedmasked','bleedmask-mini',**args)
def run_stage_2(ccd):
    ccdstring= "%02d"%int(ccd)

    #use the entire image to get astrometry solution
    scamp('D00'+str(EXPNUM)+'_'+FILTER+'_'+ccdstring+'_r'+rRun+'p'+pRun+'_sextractor.fits')
    change_head('D00'+str(EXPNUM)+'_'+FILTER+'_'+ccdstring+'_r'+rRun+'p'+pRun+'_sextractor.head', 'sextractor', 'detrend', 'wcs', CCD, **args)

    bleedmask(ccdstring,'wcs','bleedmasked',**args)
    skycompress(ccdstring,'bleedmasked','bleedmask-mini',**args)

#run "stage 3" loop over all ccds
#for ccd in CCD:
#    ccdstring= "%02d"%int(ccd)
#    skysubtract(ccd,'bleedmasked','skysub','skyfit-binned-fp',**args )
#    pixcorr_starflat('skysub', 'starflat', ccdstring,**args)
#    nullweightbkg('starflat','nullwtbkg',ccdstring,**args)
#    sextractorsky('nullwtbkg','bkg',ccdstring,**args)
#    immask(ccdstring,'starflat','immask',**args)
#    rowinterp_nullweight('immask', 'nullweightimmask', ccdstring,**args)
#    sextractorPSF('nullweightimmask', 'nullweightimmask', 'sextractor_psf', 'sextractor.psf', ccdstring, **args)	
#    read_geometry('sextractor_psf', 'regions', ccdstring, **args)
def run_stage_3(ccd):
    ccdstring= "%02d"%int(ccd)
    skysubtract(ccd,'bleedmasked','skysub','skyfit-binned-fp',**args )
    pixcorr_starflat('skysub', 'starflat', ccdstring,**args)
    #pixcorr_starflat('bleedmasked', 'starflat', ccdstring,**args)
    nullweightbkg('starflat','nullwtbkg',ccdstring,**args)
    sextractorsky('nullwtbkg','bkg',ccdstring,**args)
    immask(ccdstring,'starflat','immask',**args)
    rowinterp_nullweight('immask', 'nullweightimmask', ccdstring,**args)
    sextractorPSFEX('nullweightimmask', 'sextractorPSFEX', ccdstring, **args)
    psfex( 'sextractorPSFEX', ccdstring, **args)
    sextractorPSF('nullweightimmask', 'nullweightimmask', 'fullcat', 'sextractorPSFEX.psf', ccdstring, **args)	
    read_geometry('fullcat', 'regions', ccdstring, **args)

#def run_stage_4(ccd):
#    ccdstring= "%02d"%int(ccd)
#    sextractorPSFEX('nullweightimmask', 'sextractorPSFEX', ccdstring, **args)
#    psfex( 'sextractorPSFEX', ccdstring, **args)

#def run_stage_5(ccd):
#    ccdstring= "%02d"%int(ccd)
#    sextractorPSF('nullweightimmask', 'nullweightimmask', 'fullcat', 'sextractorPSFEX.psf', ccdstring, **args)	
#    read_geometry('fullcat', 'regions', ccdstring, **args)



if __name__ == '__main__':

# read config info
    EXPNUM =  ConfigSectionMap("General")['expnum']
    FILTER = ConfigSectionMap("General")['filter']
    NITE = ConfigSectionMap("General")['nite']
    CCD = (ConfigSectionMap("General")['chiplist']).split( ',')
    rRun = str(ConfigSectionMap("General")['r'])
    pRun = str(ConfigSectionMap("General")['p'])
    YEAR = ConfigSectionMap("General")['year']
    EPOCH = ConfigSectionMap("General")['epoch']

# setup args
    EXPFILE =  'DECam_00'+str(EXPNUM)+'.fits.fz'
    args = {'expnum': EXPNUM, 'filter': FILTER, 'ccd':'0', 'r':rRun, 'p':pRun, 'year': YEAR, 'epoch': EPOCH}
    NPARALLEL = 4

    cfg_file_copy(**args)
#run crosstalk
    crosstalk(EXPFILE,NITE,**args)

#run "stage 1" loop over all ccds
    if not os.path.exists('default.psf'):
        copy_from_Dcache(data_conf+'default.psf')

    Parallel(n_jobs=NPARALLEL)(delayed(run_stage_1)(ccd) for ccd in CCD)

    Parallel(n_jobs=NPARALLEL)(delayed(run_stage_2)(ccd) for ccd in CCD)

#use the entire image to get skyfit
    skyCombineFit('bleedmask-mini','bleedmask-mini-fp','skyfit-binned-fp',**args)

    Parallel(n_jobs=NPARALLEL)(delayed(run_stage_3)(ccd) for ccd in CCD)

#    Parallel(n_jobs=NPARALLEL)(delayed(run_stage_4)(ccd) for ccd in CCD)

#    Parallel(n_jobs=NPARALLEL)(delayed(run_stage_5)(ccd) for ccd in CCD)

#create list of ccd corners (.out file)
    for ccd in CCD:
        os.system('bash getcorners.sh '+str(EXPNUM)+' . . '+str(ccd))
# compress immask.fits files before copyback
    for immaskfile in glob.glob('*_immask.fits'):
        os.system('fpack -Y ' + str(immaskfile) )
# copy outputs to Dcache
    data_exp = ConfigSectionMap("General")['exp_dir']
    dir_nite = data_exp+NITE
    dir_final = dir_nite+'/'+EXPNUM
    os.system('ifdh mkdir '+str(dir_nite) )
    os.system('ifdh mkdir '+str(dir_final) )
    cmd = 'ifdh cp --force=xrootd -D *.out *fullcat* *_immask.fits.fz ' +str(dir_final)
    print cmd
    os.system(cmd)
