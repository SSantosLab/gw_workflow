import numpy as np
import pyfits
import os
import ConfigParser
import sys
import argparse
from despyfits.DESImage import DESImage
import subprocess
from scipy import stats

################  USAGE ##############

parser = argparse.ArgumentParser()
parser.add_argument("confFile",help="Need a configuration file.")
args = parser.parse_args()


###########  Configuration ############

Config = ConfigParser.ConfigParser()
configFile = args.confFile 
Config.read(configFile)

def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

####### Setting general stuffs #######

template_file = ConfigSectionMap("General")['template']
exp_template_file = ConfigSectionMap("General")['exp_template']
chiplist = ConfigSectionMap("General")['chiplist']
data_dir = ConfigSectionMap("General")['data_dir']
data_conf = ConfigSectionMap("General")['conf_dir']
correc_dir =  ConfigSectionMap("General")['corr_dir']
year =  ConfigSectionMap("General")['year']
yearb =  ConfigSectionMap("General")['yearb']
epoch =  ConfigSectionMap("General")['epoch']
epochb =  ConfigSectionMap("General")['epochb']


FILTER = ConfigSectionMap("General")['filter']
############  FUNCTIONS  ##############

#---------------------------#
#     copy functions       #
#---------------------------#

def copy_from_Dcache(file):
    cmd = 'ifdh cp --force=xrootd -D ' + file + ' . '

    print 'getting '+ file +' from dcache'
    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)

    return

def copy_to_Dcache(file, dir):
    cmd = 'ifdh cp --force=xrootd -D ' + file + ' ' + dir
	
    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)

    return

def make_link_or_copy(file):
    cmd = ''
    if os.path.exists(file):
        print('Path accessible; making link.')
        cmd = 'ln -sf ' + file + ' . '
    else:
        print('getting '+ file +' from dCache.')
        cmd = 'ifdh cp --force=xrootd -D ' + file + ' . '
    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)

    return

#----------------------------------------------------------#
#     this function runs crosstalk trough a flat-field     #
#----------------------------------------------------------#

xtalk_file = ConfigSectionMap("crosstalk")['xtalk']
xtalk_template = ConfigSectionMap("crosstalk")['template']
replace_file = ConfigSectionMap("crosstalk")['replace']

if not os.path.exists(replace_file):
    make_link_or_copy(data_conf+replace_file)        


def crosstalk(EXPFILE,NITE,**args):

    if not os.path.exists(xtalk_file): 
        make_link_or_copy(data_conf+xtalk_file)
                
    copy_from_Dcache(data_dir + NITE + '/' + EXPFILE)  #copy data

    cmd = 'DECam_crosstalk ' + EXPFILE + \
        ' ' + xtalk_template.format(**args) +\
	' -crosstalk ' + xtalk_file + \
	' -ccdlist ' + chiplist +\
        ' -overscanfunction 0 -overscansample 1 -overscantrim 5 ' + \
        ' -photflag 1 -verbose 0' +\
	' -replace '+ replace_file 

    print '\n',cmd,'\n'

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)

    return


#---------------------------------------------------------------#
#     this function runs pixcorrect trough crosstalk output     #
#---------------------------------------------------------------#

bias = ConfigSectionMap("pixcorrect")['bias']
bpm = ConfigSectionMap("pixcorrect")['bpm']
linearity = ConfigSectionMap("pixcorrect")['linearity']
bf = ConfigSectionMap("pixcorrect")['bf']
flat = ConfigSectionMap("pixcorrect")['flat']

if not os.path.exists(linearity):
    make_link_or_copy(correc_dir+'lin_'+str(yearb)+'/'+linearity)
    
if not os.path.exists(bf):
    make_link_or_copy(correc_dir+'bf_'+str(yearb)+'/'+bf)
 
def pixcorrect(outname, CCD, **args):
    args['ccd']=CCD	

#    copy_from_Dcache(correc_dir+'superflat_'+str(year)+'_'+str(epoch)+'/biascor/'+bias.format(**args))
    if not os.path.exists(bias.format(**args)):
        make_link_or_copy(correc_dir+'/biascor/'+str(year)+str(epoch)+'/'+bias.format(**args))

#    copy_from_Dcache(correc_dir+'superflat_'+str(year)+'_'+str(epoch)+'/norm-dflatcor/'+flat.format(**args) )
    if not os.path.exists(flat.format(**args)):
        make_link_or_copy(correc_dir+'/norm_dflatcor/'+str(year)+str(epoch)+'/'+flat.format(**args) )

#    copy_from_Dcache(correc_dir+'bpm_'+str(yearb)+'_'+str(epochb)+'/'+bpm.format(**args))
    if not os.path.exists(bpm.format(**args)):
        make_link_or_copy(correc_dir+'bpm/'+str(year)+str(epoch)+'/'+bpm.format(**args))
	

    cmd = 'pixcorrect_im --verbose --in ' + template_file.format(**args)+'_xtalk.fits' + \
        ' -o ' +template_file.format(**args)+'_'+outname+'.fits' \
	' --bias ' + bias.format(**args) + \
	' --bpm ' + bpm.format(**args) + \
	' --lincor ' +  linearity + \
	' --bf ' + bf + \
	' --gain '  + \
    	' --flat ' + flat.format(**args) + \
        ' --resaturate --fixcols --addweight'	

    print '\n',cmd,'\n'	
	
    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)

    return


#----------------------------------------#
#     this function runs mkbleedmask     #
#----------------------------------------#
#adding -L 30

def bleedmask(CCD,inname,outname,**args):
    args['ccd']=CCD
	
    cmd = 'mkbleedmask ' + template_file.format(**args)+'_'+inname+'.fits' + \
	' ' + template_file.format(**args)+'_'+outname+'.fits' + \
	'  -m -b 5 -f 1.0 -l 7 -n 7 -r 5 -t 20 -v 3 -w 2.0 -y 1.0 -s 100 -v 3 -E 6 -L 30' + \
 	'  -x ' +template_file.format(**args)+'_trailbox.fits -o ' +template_file.format(**args)+'_satstars.fits'   

    print '\n',cmd,'\n'	

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
    return	

#------------------------------#
#  skycompress mkbleedmask     #
#------------------------------#

def skycompress(CCD,inname,outname,**args):
    args['ccd']=CCD

    cmd = 'sky_compress --in ' + template_file.format(**args)+'_'+inname+'.fits' + \
	' --skyfilename ' + template_file.format(**args)+'_'+outname+'.fits' + \
	' --blocksize 128'

    print '\n',cmd,'\n'	

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
    return	

	

#----------------------------------------#
#     this function runs immask          #
#----------------------------------------#
def immask(CCD, inname, outname, **args):
    args['ccd']=CCD
	
    cmd = 'immask all ' + template_file.format(**args)+'_'+inname+'.fits' +\
 	' ' +  template_file.format(**args)+'_'+outname+'.fits' +\
	'   --minSigma 7.0 --max_angle 75  --max_width 300 --nsig_detect 18 --nsig_mask 12 --nsig_merge 12'+\
	'   --nsig_sky 1.5  --min_fill 0.33  --draw  --write_streaks  --streaksfile ' + template_file.format(**args)+'_streaksfile.fits' 

    print '\n',cmd,'\n'	

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
    return	
	

#---------------------------------------#
#            skyCombine                 #
#---------------------------------------#
PCFILENAMEPREFIX = ConfigSectionMap("skyCombineFit")['pcafileprefix']
PCFILENAME = PCFILENAMEPREFIX
#PCFILENAME = PCFILENAMEPREFIX+'_'+str(year)+'_e1_'+FILTER+'_n04.fits'
#PCFILENAME = PCFILENAMEPREFIX+'_'+str(year)+'_'+str(epoch)+'_'+FILTER+'_n04.fits'

#copy_from_Dcache(correc_dir+'skytemp_'+str(year)+'_e1/'+PCFILENAME)
#copy_from_Dcache(correc_dir+'skytemp_'+str(year)+'_'+str(epoch)+'/'+PCFILENAME)


def skyCombineFit(inputFile,skycombineFile,skyfitinfoFile,**args):

    if not os.path.exists(PCFILENAME.format(**args)):
        make_link_or_copy(correc_dir+'/skypca/'+str(year)+str(epoch)+'/'+PCFILENAME.format(**args))
    #-----------------------
    cmd = 'ls  *'+inputFile+'.fits > listpcain'
    print cmd 
    retval = subprocess.call(cmd, shell=True)

    if retval != 0:
        sys.exit(1)

    del cmd, retval

    #------------------------
    #cmd = 'sky_combine --miniskylist listpcain -o ' + exp_template_file.format(**args)+'_'+skycombineFile+'.fits --ccdnums 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,62 --invalid S30,N30' 
    cmd = 'sky_combine --miniskylist listpcain -o ' + exp_template_file.format(**args)+'_'+skycombineFile+'.fits --ccdnums'+ccdlist+' --invalid S30,N30' 
    print cmd
    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)

    if retval != 0:
        sys.exit(1)

    del cmd, retval

    #----------------------
    cmd = 'sky_fit --infilename '+  exp_template_file.format(**args)+'_'+skycombineFile+'.fits' +\
    ' --outfilename '+ exp_template_file.format(**args)+'_'+skyfitinfoFile+'.fits --pcfilename  '+ (PCFILENAME[PCFILENAME.rfind("/")+1:]).format(**args)
    print cmd
    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)

    if retval != 0:
        sys.exit(1)


#----------------------------------------#
#              starflat                  # 
#----------------------------------------#      

starflat = ConfigSectionMap("starflat")['starflat']

def pixcorr_starflat(inname, outname, CCD,**args):
    args['ccd']=CCD
    if not os.path.exists(starflat.format(**args)):
        make_link_or_copy(correc_dir+'/starflat/'+str(year)+str(epoch)+'/'+starflat.format(**args))
#    copy_from_Dcache(correc_dir+'starflat_'+str(year)+'_'+str(epoch)+'/'+starflat.format(**args))
    #copy_from_Dcache(correc_dir+'starflat_'+str('y2')+'_'+str('e2')+'/'+starflat.format(**args))
        
    cmd = 'pixcorrect_im --verbose --in ' +template_file.format(**args)+'_'+inname+'.fits' +\
        ' --starflat '+ starflat.format(**args) +\
        ' --out '+template_file.format(**args)+'_'+outname+'.fits'

    print '\n',cmd,'\n'

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)

    if retval != 0:
        sys.exit(1)



#--------------------------------------------#
#                skysubtract                 #    
#-----------------------------------------  -#
pc_filename  =  ConfigSectionMap("skysubtract")['pcfilename']
weight = ConfigSectionMap("skysubtract")['weight']

def skysubtract(CCD, inname, outname, skyfitinfoFile,**args ):
    args['ccd']=CCD
    if not os.path.exists(pc_filename.format(**args)):
        make_link_or_copy(correc_dir+'/skypca/'+str(year)+str(epoch)+'/skytemp/'+pc_filename.format(**args))
#    copy_from_Dcache(correc_dir+'skytemp_'+str(year)+'_'+str(epoch)+'/'+pc_filename.format(**args))

    print template_file.format(**args)
    cmd = 'sky_subtract -i  '  +template_file.format(**args)+'_'+inname+'.fits'+\
        ' -o ' + template_file.format(**args)+'_'+outname+'.fits' +\
        ' --fitfilename '+ exp_template_file.format(**args)+'_'+skyfitinfoFile+'.fits ' +\
        ' --pcfilename ' +pc_filename.format(**args) +\
        ' --domefilename ' +flat.format(**args) +\
        ' --weight  ' +weight

    print '\n',cmd,'\n'

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
    return


#------------------------------------------#
#              scamp                       #
#------------------------------------------#

imagflags = ConfigSectionMap("scamp")['imagflags']
flag_mask = ConfigSectionMap("scamp")['flag_mask']
flag_astr = ConfigSectionMap("scamp")['flag_astr']
catalog_ref = ConfigSectionMap("scamp")['catalog_ref']
default_scamp = ConfigSectionMap("scamp")['default_scamp']
head_file = ConfigSectionMap("scamp")['head']


farg = {'filter': FILTER}
head_FILE =  head_file.format(**farg)

if "," in head_FILE:
    # assumes name format fX,X.head
    ccdlist = head_FILE[1:head_FILE.index(".")]
    ccdlist = ccdlist.split(",")
    if not os.path.exists(head_FILE):
        openfile = open(head_FILE, 'w')
        openfile.close()
    outfile = open(head_FILE, 'a')
    for ccd in ccdlist:
        tempfile = "f%s.head"%ccd
        if not os.path.exists(tempfile):
            make_link_or_copy(data_conf+tempfile)
        with open(tempfile) as infile:
            outfile.write(infile.read())
            infile.close()
    outfile.close()

if not os.path.exists(default_scamp):
    make_link_or_copy(data_conf+default_scamp)
if not os.path.exists(head_FILE):
    make_link_or_copy(data_conf+head_FILE)

def scamp(inputFile):
    cmd = 'scamp ' + inputFile +\
        ' ' + '-AHEADER_GLOBAL ' + head_FILE +\
        ' -ASTRINSTRU_KEY DUMMY -AHEADER_SUFFIX .aheadnoexist -ASTREFMAG_LIMITS -99,17 ' +\
        ' -ASTREF_CATALOG ' +catalog_ref +' -c ' +default_scamp +\
        ' -WRITE_XML Y -XML_NAME scamp.xml -MOSAIC_TYPE SAME_CRVAL -ASTREF_BAND DEFAULT -POSITION_MAXERR 60.0 -NTHREADS 1 '

    print '\n',cmd,'\n'

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
    return

#-----------------------------------------#
#           combineFiles                          
#----------------------------------------#-
def combineFiles(inputFile, outputFile):

    cmd = 'fitscombine '+inputFile+' ' +outputFile
    retval = subprocess.call(cmd, shell=True)

    if retval != 0:
        sys.exit(1)
    return

#-----------------------------------------#
#             sextractor                  #
#-----------------------------------------#
sexnnwFile = ConfigSectionMap("sextractor")['starnnw_name']
sexconvFile = ConfigSectionMap("sextractor")['filter_name']
sexparamFile  = ConfigSectionMap("sextractor")['parameters_name']
configFile = ConfigSectionMap("sextractor")['configfile']

if not os.path.exists(sexnnwFile):
    make_link_or_copy(data_conf+sexnnwFile)
if not os.path.exists(sexconvFile):
    make_link_or_copy(data_conf+sexconvFile)
if not os.path.exists(sexparamFile):
    make_link_or_copy(data_conf+sexparamFile)
if not os.path.exists(configFile):
    make_link_or_copy(data_conf+configFile)
if not os.path.exists('default.psf'):
    make_link_or_copy(data_conf+'default.psf')


def sextractor(inname, outname, CCD, **args):
    args['ccd']=CCD

    cmd = 'sex ' + template_file.format(**args)+'_'+inname+'.fits[0]'+\
        ' -c  ' + configFile + ' -FILTER_NAME ' + sexconvFile + ' -STARNNW_NAME ' +sexnnwFile + ' -CATALOG_NAME  ' + template_file.format(**args)+'_'+outname+'.fits'+\
        ' -FLAG_IMAGE '  + template_file.format(**args)+'_'+inname+'.fits[1] -PARAMETERS_NAME ' + sexparamFile +\
        ' -DETECT_THRESH 10.0 -SATUR_KEY SATURATE  -CATALOG_TYPE FITS_LDAC -WEIGHT_IMAGE  ' + template_file.format(**args)+'_'+inname+'.fits[2]'+\
        '  -WEIGHT_TYPE  MAP_WEIGHT  '

    print '\n',cmd,'\n'

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
    return

#------------------------------------------#
#        sextractor for PSFEX              #
#------------------------------------------#


def sextractorPSFEX(inname, outname, CCD, **args):
    args['ccd']=CCD
    confPSF = ConfigSectionMap("sextractor")['sexforpsfexconfigfile']
    sexparamPSFEXFile = ConfigSectionMap("sextractor")['parameters_name_psfex']
    if not os.path.exists(confPSF):
        make_link_or_copy(data_conf+confPSF)
    if not os.path.exists(sexparamPSFEXFile):
        make_link_or_copy(data_conf+sexparamPSFEXFile)
    cmd = 'sex ' + template_file.format(**args)+'_'+inname+'.fits[0]'+\
    ' -c  ' + confPSF + ' -FILTER_NAME ' + sexconvFile + ' -STARNNW_NAME ' +sexnnwFile + ' -CATALOG_NAME  ' + template_file.format(**args)+'_'+outname+'.fits'+\
    ' -FLAG_IMAGE '  + template_file.format(**args)+'_'+inname+'.fits[1] -PARAMETERS_NAME ' +sexparamPSFEXFile  +\
    '  -SATUR_KEY SATURATE  -CATALOG_TYPE FITS_LDAC -WEIGHT_IMAGE  ' + template_file.format(**args)+'_'+inname+'.fits[2]'+\
    '  -WEIGHT_TYPE  MAP_WEIGHT  '

    print '\n',cmd,'\n'

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
        return

#-----------------------------------#
#  nulling weights for sky ccd31    #
#-----------------------------------#

def nullweightbkg(inname, outname,CCD,**args):
    args['ccd']=CCD
    # Preprocess the image for SExtractor by nulling weights in bad regions
    cmd = 'null_weights -i ' + template_file.format(**args)+'_'+inname+'.fits' \
          ' -o ' + template_file.format(**args)+'_'+outname+'.fits' + \
          ' --null_mask BADAMP,EDGEBLEED,EDGE,STAR,TRAIL' 

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)

#---------------------------------------------
#	sextractorsky bkg calculator
#---------------------------------------------

make_link_or_copy(data_conf+'sex.param_bkg')
sexbkgparamFile='sex.param_bkg'

def sextractorsky(inname, outname, CCD, **args):
    args['ccd']=CCD

    cmd = 'sex ' + template_file.format(**args)+'_'+inname+'.fits[0]'+\
        ' -c  ' + configFile +' -CHECKIMAGE_TYPE BACKGROUND ' +\
	' -DETECT_THRESH 1000 -FILTER N ' +\
	' -CHECKIMAGE_NAME ' + template_file.format(**args)+'_'+outname+'.fits'+\
	' -WEIGHT_TYPE MAP_WEIGHT -WEIGHT_IMAGE '+template_file.format(**args)+'_'+inname+'.fits[2],'+template_file.format(**args)+'_'+inname+'.fits[2]' +\
        ' -PARAMETERS_NAME ' + sexbkgparamFile +\
        ' -CATALOG_TYPE NONE -INTERP_TYPE ALL -INTERP_MAXXLAG 16 -INTERP_MAXYLAG 16 '

    print '\n',cmd,'\n'
    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
    return

#----------------------------------------#
#              psfex                     #   
#----------------------------------------#
config_filePSF =  ConfigSectionMap("psfex")['configfile']
if not os.path.exists(config_filePSF):
    make_link_or_copy(data_conf+config_filePSF)

def psfex(name,CCD, **args):
    args['ccd']=CCD

    cmd = 'psfex ' + template_file.format(**args)+'_'+name+'.fits -c  ' +config_filePSF +\
   '  -OUTCAT_NAME  '+ template_file.format(**args)+'_psflist.fits  -OUTCAT_TYPE FITS_LDAC'
    print '\n',cmd,'\n'

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)

    return

#-----------------------------------------#
#       sextractor  with   psf            #
#-----------------------------------------#
sexparamFile_2  = ConfigSectionMap("sextractor")['parameters_name2']
configFile2 = ConfigSectionMap("sextractor")['configfile2']
sexconvFile2 = ConfigSectionMap("sextractor")['filter_name2']


print sexconvFile2
if not os.path.exists(sexconvFile2):
    make_link_or_copy(data_conf+sexconvFile2)
if not os.path.exists(sexparamFile_2):
    make_link_or_copy(data_conf+sexparamFile_2)
if not os.path.exists(configFile2):
    make_link_or_copy(data_conf+configFile2)
if not os.path.exists('default.psf'):
    make_link_or_copy(data_conf+'default.psf')


def sextractorPSF(name, name1, outname, filepsf, CCD, **args):
    args['ccd']=CCD

    h=DESImage.load(template_file.format(**args)+'_'+str(name)+'.fits')
    fwhm = 0.263*float(h.header['FWHM'])


    cmd = 'sex ' + template_file.format(**args)+'_'+name+'.fits[0]'+\
        ' -PSF_NAME ' + template_file.format(**args)+'_'+filepsf+\
        ' -c  ' + configFile2 + ' -FILTER_NAME ' + sexconvFile2 + ' -STARNNW_NAME ' +sexnnwFile + ' -CATALOG_NAME  ' + template_file.format(**args)+'_'+outname+'.fits'+\
        ' -FLAG_IMAGE '  + template_file.format(**args)+'_'+name+'.fits[1] -PARAMETERS_NAME ' + sexparamFile_2 +\
        ' -INTERP_TYPE VAR_ONLY  -INTERP_MAXXLAG 4 -INTERP_MAXYLAG 4  -SEEING_FWHM ' + str(fwhm) +\
	' -DETECT_THRESH 1.5 -SATUR_KEY SATURATE  -CATALOG_TYPE FITS_LDAC -WEIGHT_IMAGE '+\
        template_file.format(**args)+'_'+name1+'.fits[2],'+template_file.format(**args)+'_'+name1+'.fits[2]'+\
        '  -WEIGHT_TYPE MAP_WEIGHT  -CHECKIMAGE_NAME ' + template_file.format(**args)+'_segmap.fits -CHECKIMAGE_TYPE SEGMENTATION'


    print '\n',cmd,'\n'

    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
    return

#--------------------------------------#
#     fwhm routine from Wrappers      #
#--------------------------------------#

debug = 0
def fwhm(incat):
    """
    Get the median FWHM and ELLIPTICITY from the scamp catalog (incat)
    """
    CLASSLIM = 0.75      # class threshold to define star
    MAGERRLIMIT = 0.1  # mag error threshold for stars

    if debug: print "!!!! WUTL_STS: (fwhm): Opening scamp_cat to calculate median FWHM & ELLIPTICITY.\n"
    hdu = pyfits.open(incat,"readonly")

    if debug: print "!!!! WUTL_STS: (fwhm): Checking to see that hdu2 in scamp_cat is a binary table.\n"
    if 'XTENSION' in hdu[2].header:
        if hdu[2].header['XTENSION'] != 'BINTABLE':
            print "!!!! WUTL_ERR: (fwhm): this HDU is not a binary table"
            exit(1)
    else:
        print "!!!! WUTL_ERR: (fwhm): XTENSION keyword not found"
        exit(1)

    if 'NAXIS2' in hdu[2].header:
        nrows = hdu[2].header['NAXIS2']
        print "!!!! WUTL_INF: (fwhm): Found %s rows in table" % nrows
    else:
        print "!!!! WUTL_ERR: (fwhm): NAXIS2 keyword not found"
        exit(1)

    tbldct = {}
    for colname in ['FWHM_IMAGE','ELLIPTICITY','FLAGS','MAGERR_AUTO','CLASS_STAR']:
        if colname in hdu[2].columns.names:
            tbldct[colname] = hdu[2].data.field(colname)
        else:
            print "!!!! WUTL_ERR: (fwhm): No %s column in binary table" % colname
            exit(1)

    hdu.close()

    flags = tbldct['FLAGS']
    cstar = tbldct['CLASS_STAR']
    mgerr = tbldct['MAGERR_AUTO']
    fwhm = tbldct['FWHM_IMAGE']
    ellp = tbldct['ELLIPTICITY']

    fwhm_sel = []
    ellp_sel = []
    count = 0
    for i in range(nrows):
        if flags[i] < 1 and cstar[i] > CLASSLIM and mgerr[i] < MAGERRLIMIT and fwhm[i]>0.5 and ellp[i]>=0.0:
            fwhm_sel.append(fwhm[i])
            ellp_sel.append(ellp[i])
            count+=1

    fwhm_sel.sort()
    ellp_sel.sort()

    # allow the no-stars case count = 0 to proceed without crashing
    if count <= 0:
      fwhm_med = 4.0
      ellp_med = 0.0
    else:
      if count%2:
        # Odd number of elements
        fwhm_med = fwhm_sel[count/2]
        ellp_med = ellp_sel[count/2]
      else:
        # Even number of elements
        fwhm_med = 0.5 * (fwhm_sel[count/2]+fwhm_sel[count/2-1])
        ellp_med = 0.5 * (ellp_sel[count/2]+ellp_sel[count/2-1])

    if debug:
        print "FWHM=%.4f" % fwhm_med
        print "ELLIPTIC=%.4f" % ellp_med
        print "NFWHMCNT=%s" % count

    return (fwhm_med,ellp_med,count)	
	

#------------------------------------#
#        changing head               #
#------------------------------------#

def change_head(File, catalog, image, outname, CCD, **args):

    ccdLen = len(CCD)

    #Getting the data and saving into an array
    o = open(File,'r').read().splitlines()
    info_array = ['CRVAL1','CRVAL2','CRPIX1','CRPIX2','CD1_1','CD1_2','CD2_1','CD2_2',\
                  'PV1_0','PV1_1 ','PV1_2','PV1_4','PV1_5','PV1_6','PV1_7','PV1_8','PV1_9','PV1_10',\
                  'PV2_0','PV2_1 ','PV2_2','PV2_4','PV2_5','PV2_6','PV2_7','PV2_8','PV2_9','PV2_10']

    n = len(info_array)
    matrix = []
    for ii in o:
        for oo in info_array:
            if oo in ii.split('=')[0] :
                #print ii.split('=')[0], ii.split('=')[1].split('/')[0]
                matrix.append(ii.split('=')[1].split('/')[0])

    matrix = np.array(matrix)

    #changing the header
    cont = 0
    for i in range(ccdLen):
	ccdstring="%02d"%int(CCD[i])
        args['ccd']=ccdstring
        catalog1 = template_file.format(**args)+'_'+catalog+'.fits'
        image1 = template_file.format(**args)+'_'+image+'.fits'

        fwhm_, ellip, count = fwhm(catalog1)

        h=DESImage.load(image1)
        h.header['FWHM'] = fwhm_
        h.header['ELLIPTIC'] = ellip
	h.header['SCAMPFLG'] = 0		

        im=h.data
        iterate1=stats.sigmaclip(im,5,5)[0]
        iterate2=stats.sigmaclip(iterate1,5,5)[0]
        iterate3=stats.sigmaclip(iterate2,3,3)[0]
        skybrite=np.median(iterate3)
        skysigma=np.std(iterate3)

        h.header['SKYBRITE'] = skybrite
        h.header['SKYSIGMA'] = skysigma
        h.header['CAMSYM'] = 'D'
        h.header['SCAMPCHI'] = 0.0
        h.header['SCAMPNUM'] = 0

        for j in range(n):
            h.header[info_array[j]] = float(matrix[cont])
	    cont =  cont + 1

            h.save(template_file.format(**args)+'_'+outname+'.fits')

#-----------------------------------#
#  nulling weights in bad regions   #
#-----------------------------------#

def rowinterp_nullweight(inname, outname,CCD,**args):
    args['ccd']=CCD
    # Preprocess the image for SExtractor by interpolating (temporarily) across bleedtrails
    cmd = 'rowinterp_nullweight -i ' + template_file.format(**args)+'_'+inname+'.fits' \
          ' -o ' + template_file.format(**args)+'_'+outname+'.fits' + \
          ' -l ' + template_file.format(**args)+'_'+outname+'.log' + \
          ' --interp_mask TRAIL --invalid_mask EDGE --max_cols 50 -vv --null_mask ' + \
	  ' BADAMP,EDGEBLEED,EDGE,CRAY'
    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)


#-----------------------------------#
#  nulling weights in bad regions   #
#-----------------------------------#

def nullweight(inname, outname,CCD,**args):
    args['ccd']=CCD
    # Preprocess the image for SExtractor by nulling weights in bad regions
    cmd = 'null_weights -i ' + template_file.format(**args)+'_'+inname+'.fits' \
          ' -o ' + template_file.format(**args)+'_'+outname+'.fits' + \
          ' -l ' + template_file.format(**args)+'_'+outname+'.log' + \
          ' --null_mask BADAMP,EDGEBLEED,EDGE' + \
          ' --resaturate'
    retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
    if retval != 0:
        sys.exit(1)
#--------------------------------#
#      creating regions          #
#--------------------------------#

def read_geometry(inname, outname, CCD, **args):

    args['ccd']=CCD	
    file = template_file.format(**args)+'_'+inname+'.fits'

    cat = pyfits.open(file)
    XWIN_IMAGE = cat[2].data.field('XWIN_IMAGE').copy()
    YWIN_IMAGE = cat[2].data.field('YWIN_IMAGE').copy()
    A_IMAGE = cat[2].data.field('A_IMAGE').copy()
    B_IMAGE = cat[2].data.field('B_IMAGE').copy()
    THETA_IMAGE = cat[2].data.field('THETA_IMAGE').copy()
    SPREAD_MODEL = cat[2].data.field('SPREAD_MODEL').copy()
    FLAGS = cat[2].data.field('FLAGS').copy()
    IMAFLAGS_ISO = cat[2].data.field('IMAFLAGS_ISO').copy() 
    del cat
 
    c = 5
    A_IMAGE = c*A_IMAGE
    B_IMAGE = c*B_IMAGE

    color = []
    ellipse = []
    widht = []   


    N = len(SPREAD_MODEL)
    for i in range(N):
        ellipse.append("ellipse")
	if FLAGS[i]<4:
	    if IMAFLAGS_ISO[i] == 0:
	        if SPREAD_MODEL[i] < 0.003 and SPREAD_MODEL[i] > -0.003:	
                    color.append("#color=red") #stars
		    widht.append('width = 4')
		elif SPREAD_MODEL[i] >= 0.003:
		    color.append("#color=yellow") #galaxies
		    widht.append('width = 4')		
	        else:
	            color.append("#color=green") #garbage
		    widht.append('width = 2')
	    elif IMAFLAGS_ISO[i]  & 32 !=0:
		    color.append("#color=blue") #detection near a bright stars.
		    widht.append('width = 3')
	    else:
		color.append("#color=cyan") #object with questionable flags  Flags >=4
                widht.append('width = 3')
	else: 
	    color.append("#color=magenta") #object with questionable flags  Flags >=4
	    widht.append('width = 2')

    DAT =  np.column_stack((ellipse, XWIN_IMAGE, YWIN_IMAGE, A_IMAGE, B_IMAGE, THETA_IMAGE, color, widht))
    np.savetxt(template_file.format(**args)+'_'+outname+'.reg', DAT, delimiter=" ", fmt="%s")   

def cfg_file_copy(**args):
    confPSF = ConfigSectionMap("sextractor")['sexforpsfexconfigfile']
    sexparamPSFEXFile = ConfigSectionMap("sextractor")['parameters_name_psfex']
    if not os.path.exists(confPSF):
        make_link_or_copy(data_conf+confPSF)
    if not os.path.exists(sexparamPSFEXFile):
        make_link_or_copy(data_conf+sexparamPSFEXFile)
    if not os.path.exists(sexparamPSFEXFile):
        make_link_or_copy(correc_dir+'/skypca/'+str(year)+str(epoch)+'/'+PCFILENAME.format(**args))
    if not os.path.exists('default.psf'):
        make_link_or_copy(data_conf+'default.psf')
