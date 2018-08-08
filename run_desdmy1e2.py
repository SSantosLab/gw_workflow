from desdmLiby1e2 import * 
import numpy as np
import os

#just testing in  fews ccds
EXPNUM =  ConfigSectionMap("General")['expnum']
FILTER = ConfigSectionMap("General")['filter']
NITE = ConfigSectionMap("General")['nite']
CCD = (ConfigSectionMap("General")['chiplist']).split( ',')
rRun = str(ConfigSectionMap("General")['r'])
pRun = str(ConfigSectionMap("General")['p'])
YEAR = ConfigSectionMap("General")['year']
EPOCH = ConfigSectionMap("General")['epoch']

EXPFILE =  'DECam_00'+str(EXPNUM)+'.fits.fz'
args = {'expnum': EXPNUM, 'filter': FILTER, 'ccd':'0', 'r':rRun, 'p':pRun, 'year': YEAR, 'epoch': EPOCH}

#running crosstalk
crosstalk(EXPFILE,NITE,**args)

#running pixelcorrect ans bleedmask
copy_from_Dcache(data_conf+'default.psf')
for ccd in CCD:
    ccdstring= "%02d"%int(ccd)
    pixcorrect('detrend',ccdstring,**args)
    nullweight('detrend', 'nullweight', ccdstring,**args)
    sextractor('nullweight', 'sextractor', ccdstring, **args)
#    psfex( 'sextractor', ccdstring, **args)

for ccd in CCD:
    ccdstring= "%02d"%int(ccd)

    combineFiles('D00'+str(EXPNUM)+'_i_'+ccdstring+'_r4p5_sextractor.fits', 'Scamp_allCCD_r'+rRun+'p'+pRun+'.fits')
    scamp('D00'+str(EXPNUM)+'_i_'+ccdstring+'_r4p5_sextractor.fits')
    change_head('D00'+str(EXPNUM)+'_i_'+ccdstring+'_r4p5_sextractor.head', 'sextractor', 'detrend', 'wcs', CCD, **args)

    bleedmask(ccdstring,'wcs','bleedmasked',**args)
    skycompress(ccdstring,'bleedmasked','bleedmask-mini',**args)


skyCombineFit('bleedmask-mini','bleedmask-mini-fp','skyfit-binned-fp',**args)

for ccd in CCD:
    ccdstring= "%02d"%int(ccd)
    skysubtract(ccd,'bleedmasked','skysub','skyfit-binned-fp',**args )
    pixcorr_starflat('skysub', 'starflat', ccdstring,**args)
    nullweightbkg('starflat','nullwtbkg',ccdstring,**args)
    sextractorsky('nullwtbkg','bkg',ccdstring,**args)
    immask(ccdstring,'starflat','immask',**args)
    rowinterp_nullweight('immask', 'nullweightimmask', ccdstring,**args)
    sextractorPSFEX('nullweightimmask', 'sextractorPSFEX', ccdstring, **args)
    psfex( 'sextractorPSFEX', ccdstring, **args)
    sextractorPSF('nullweightimmask', 'nullweightimmask', 'fullcat', 'sextractorPSFEX.psf', ccdstring, **args)	
    read_geometry('fullcat', 'regions', ccdstring, **args)

# copy to  Dcache
data_exp = ConfigSectionMap("General")['exp_dir']
dir_nite = data_exp+NITE
os.system('ifdh mkdir '+str(dir_nite) )

dir_final = dir_nite+'/'+EXPNUM

for ccd in CCD:
    # make a .out file per ccd
    os.system('bash getcorners.sh '+str(EXPNUM)+' . . '+str(ccd)) 

os.system('ifdh mkdir '+str(dir_final) )


#cmd = 'ifdh  cp -D  D*r'+str(rRun)+'p'+str(pRun)+'*fp.fits D*r'+str(rRun)+'p'+str(pRun)+'_sextractor* D*r'+str(rRun)+'p'+str(pRun)+'_psflist.fits D*r'+str(rRun)+'p'+str(pRun)+'_regions.reg D*r'+str(rRun)+'p'+str(pRun)+'_immask.fits D*r'+str(rRun)+'p'+str(pRun)+'_bkg.fits D*r'+str(rRun)+'p'+str(pRun)+'_nullweight* ' +str(dir_final)

cmd = 'ifdh cp -D *.out *sextractor* *fullcat* *immask* D*_ZP.fits ' +str(dir_final)

print cmd

os.system(cmd)
