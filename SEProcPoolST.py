#!/usr/bin/env python
#
# A test of `multiprocessing.Pool` class
#
# Copyright (c) 2006-2008, R Oudkerk
# All rights reserved.
#

import multiprocessing
import time
import random
import sys
from cStringIO import  StringIO
import string
import logging
import numpy as np
import pyfits
import os
import ConfigParser

import argparse
from despyfits.DESImage import DESImage
import subprocess
from scipy import stats
import getopt
import glob
from matplotlib.rcsetup import ValidateInStrings
#import multiprocessing
import pathos
from pathos.multiprocessing import ProcessingPool as Pool
from scipy.weave.catalog import os_dependent_catalog_name
from setuptools.command.easy_install import sys_executable
import shutil



def runL1P(args):
    SEproc, x = args # unpack args
    return SEproc.l1p(x)


def runL2P(args):
    SEproc, x = args # unpack args
    return SEproc.l2p(x)

def runL3P(args):
    SEproc, x = args # unpack args
    return SEproc.l3p(x)

class SEProcPoolST():

    def __init__(self,confFile):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(confFile,help="Need a configuration file.")
        self.args = self.parser.parse_args()
        self.inargs = {}


###########  Configuration ############

        self.Config = ConfigParser.ConfigParser()
        self.configFile = self.args.confFile 
        self.Config.read(self.configFile)
        self.template_file = self.ConfigSectionMap("General")['template']
        self.exp_template_file = self.ConfigSectionMap("General")['exp_template']
        self.chiplist = self.ConfigSectionMap("General")['chiplist']

        self.data_dir = self.ConfigSectionMap("General")['data_dir']
        self.data_file = self.ConfigSectionMap("General")['data_file']
        self.data_conf = self.ConfigSectionMap("General")['conf_dir']
        self.correc_dir =  self.ConfigSectionMap("General")['corr_dir']
        self.year =  self.ConfigSectionMap("General")['year']
        self.yearb =  self.ConfigSectionMap("General")['yearb']
        self.epoch =  self.ConfigSectionMap("General")['epoch']
        self.epochb =  self.ConfigSectionMap("General")['epochb']
        self.FILTER = self.ConfigSectionMap("General")['filter']
        #--------------------------------------------------------#
        # crosstalk                                              #
        #--------------------------------------------------------#
        self.xtalk_file = self.ConfigSectionMap("crosstalk")['xtalk']
        self.xtalk_template = self.ConfigSectionMap("crosstalk")['template']
        self.replace_file = self.ConfigSectionMap("crosstalk")['replace']
        #-----------------------------------------------------------#
        #             pixcorrect                                    #
        #-----------------------------------------------------------#
        self.bias = self.ConfigSectionMap("pixcorrect")['bias']
        self.bpm = self.ConfigSectionMap("pixcorrect")['bpm']
        self.linearity = self.ConfigSectionMap("pixcorrect")['linearity']
        self.bf = self.ConfigSectionMap("pixcorrect")['bf']
        self.flat =self.ConfigSectionMap("pixcorrect")['flat']
        self.link_from_Dcache(self.correc_dir+'lin_'+str(self.yearb)+'/'+self.linearity)
        self.link_from_Dcache(self.correc_dir+'bf_'+str(self.yearb)+'/'+self.bf)
        #---------------------------------------#
        #            skyCombine                 #
        #---------------------------------------#
        self.PCFILENAMEPREFIX = self.ConfigSectionMap("skyCombineFit")['pcafileprefix']
        self.PCFILENAME = self.PCFILENAMEPREFIX
        #   starflat  #
        self.starflat = self.ConfigSectionMap("starflat")['starflat']
        # skySubtract
        self.pc_filename  =  self.ConfigSectionMap("skysubtract")['pcfilename']
        self.weight = self.ConfigSectionMap("skysubtract")['weight']
        #------------------------------------------#
        #              scamp                       #
        #------------------------------------------#
        self.imagflags = self.ConfigSectionMap("scamp")['imagflags']
        self.flag_mask = self.ConfigSectionMap("scamp")['flag_mask']
        self.flag_astr = self.ConfigSectionMap("scamp")['flag_astr']
        self.catalog_ref = self.ConfigSectionMap("scamp")['catalog_ref']
        self.default_scamp = self.ConfigSectionMap("scamp")['default_scamp']
        self.head_file = self.ConfigSectionMap("scamp")['head']
        self.farg = {'filter':self.FILTER}
        self.head_FILE =  self.head_file.format(**self.farg)
        #
        self.link_from_Dcache(self.data_conf+self.default_scamp)
        self.link_from_Dcache(self.data_conf+self.head_FILE)
        #-----------------------------------------#
        #             sextractor                  #
        #-----------------------------------------#
        self.sexnnwFile = self.ConfigSectionMap("sextractor")['starnnw_name']
        self.sexconvFile = self.ConfigSectionMap("sextractor")['filter_name']
        self.sexparamFile  = self.ConfigSectionMap("sextractor")['parameters_name']
        self.sexparamPSFEXFile  =  self.ConfigSectionMap("sextractor")['parameters_name_psfex']
        self.configFile = self.ConfigSectionMap("sextractor")['configfile']
        self.confPSF = self.ConfigSectionMap("sextractor")['sexforpsfexconfigfile']

        self.link_from_Dcache(self.data_conf+self.sexnnwFile)
        self.link_from_Dcache(self.data_conf+self.sexconvFile)
        self.link_from_Dcache(self.data_conf+self.sexparamFile)
        self.link_from_Dcache(self.data_conf+self.sexparamPSFEXFile) 
        self.link_from_Dcache(self.data_conf+self.configFile)
        self.link_from_Dcache(self.data_conf+'default.psf')
        self.link_from_Dcache(self.data_conf+self.confPSF)
        self.link_from_Dcache(self.data_conf+'sex.param_bkg')
        self.sexbkgparamFile='sex.param_bkg'
        #----------------------------------------#
        #              psfex                     #   
        #----------------------------------------#
        self.config_filePSF =  self.ConfigSectionMap("psfex")['configfile']
        self.link_from_Dcache(self.data_conf+self.config_filePSF)

        
        #-----------------------------------------#
        #       sextractor  with   psf            #
        #-----------------------------------------#
        self.sexparamFile_2  = self.ConfigSectionMap("sextractor")['parameters_name2']
        self.configFile2 = self.ConfigSectionMap("sextractor")['configfile2']
        self.sexconvFile2 = self.ConfigSectionMap("sextractor")['filter_name2']

        self.link_from_Dcache(self.data_conf+self.sexconvFile2)
        self.link_from_Dcache(self.data_conf+self.sexparamFile_2)
        self.link_from_Dcache(self.data_conf+self.configFile2)
        self.link_from_Dcache(self.data_conf+'default.psf')

        
    def ConfigSectionMap(self,section):
        dict1 = {}
        options = self.Config.options(section)
        for option in options:
            try:
                dict1[option] = self.Config.get(section, option)
                if dict1[option] == -1:
#                DebugPrint("skip: %s" % option)
                    print "skip: %s" % option
            except:
                print("exception on %s!" % option)
                dict1[option] = None
        return dict1

#---------------------------#
#       get link from stash #
#---------------------------#
    def link_from_Dcache(self,fileN):
#        print "link file %s \n" % fileN
        dest = fileN.split('/')[-1]
        if  not os.path.exists("./"+dest):
            os.symlink(fileN,dest)

        return

#---------------------------#
#     copy functions       #
#---------------------------#

    def copy_from_Dcache(self,fileN):
        cmd = 'ifdh cp -D ' + fileN + ' . '
        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)

        return

    def copy_to_Dcache(self, fileN, dirN):
        cmd = 'ifdh cp -D ' + fileN + ' ' + dirN
        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)

        return


#----------------------------------------------------------#
#     this function runs crosstalk trough a flat-field     #
#----------------------------------------------------------#


    def crosstalk(self,EXPFILE,NITE,**args):

        self.link_from_Dcache(self.data_conf+self.xtalk_file) #copy xtalk file
        self.copy_from_Dcache(self.data_dir + NITE + '/' + EXPFILE)  #copy data
        self.link_from_Dcache(self.data_conf+self.replace_file)
        cmd = 'DECam_crosstalk ' + EXPFILE + \
        ' ' + self.xtalk_template.format(**args) +\
        ' -crosstalk ' + self.xtalk_file + \
        ' -ccdlist ' + self.chiplist +\
        ' -overscanfunction 0 -overscansample 1 -overscantrim 5 ' + \
        ' -photflag 1 -verbose 0' +\
        ' -replace '+ self.replace_file 

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
        os.remove(EXPFILE)
        return


#---------------------------------------------------------------#
#     this function runs pixcorrect trough crosstalk output     #
#---------------------------------------------------------------#

 
    def pixcorrect(self,outname, CCD, **args):
        args['ccd']=CCD    

#    copy_from_Dcache(correc_dir+'superflat_'+str(year)+'_'+str(epoch)+'/biascor/'+bias.format(**args))
        self.link_from_Dcache(self.correc_dir+'/biascor/'+str(self.year)+str(self.epoch)+'/'+self.bias.format(**args))

#    copy_from_Dcache(correc_dir+'superflat_'+str(year)+'_'+str(epoch)+'/norm-dflatcor/'+flat.format(**args) )
        self.link_from_Dcache(self.correc_dir+'/norm_dflatcor/'+str(self.year)+str(self.epoch)+'/'+self.flat.format(**args) )

#    copy_from_Dcache(correc_dir+'bpm_'+str(yearb)+'_'+str(epochb)+'/'+bpm.format(**args))
        self.link_from_Dcache(self.correc_dir+'bpm/'+str(self.year)+str(self.epoch)+'/'+self.bpm.format(**args))
    
        cmd = 'pixcorrect_im --verbose --in ' + self.template_file.format(**args)+'_xtalk.fits' + \
        ' -o ' +self.template_file.format(**args)+'_'+outname+'.fits' \
        ' --bias ' + self.bias.format(**args) + \
        ' --bpm ' + self.bpm.format(**args) + \
        ' --lincor ' +  self.linearity + \
        ' --bf ' + self.bf + \
        ' --gain '  + \
        ' --flat ' + self.flat.format(**args) + \
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

    def bleedmask(self,CCD,inname,outname,**args):
        args['ccd']=CCD
    
        cmd = 'mkbleedmask ' + self.template_file.format(**args)+'_'+inname+'.fits' + \
        ' ' + self.template_file.format(**args)+'_'+outname+'.fits' + \
        '  -m -b 5 -f 1.0 -l 7 -n 7 -r 5 -t 20 -v 3 -w 2.0 -y 1.0 -s 100 -v 3 -E 6 -L 30' + \
        '  -x ' +self.template_file.format(**args)+'_trailbox.fits -o ' +self.template_file.format(**args)+'_satstars.fits'   

        print '\n',cmd,'\n'    

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
        os.remove(self.template_file.format(**args)+'_'+inname+'.fits')
        return    

#------------------------------#
#  skycompress mkbleedmask     #
#------------------------------#

    def skycompress(self,CCD,inname,outname,**args):
        args['ccd']=CCD

        cmd = 'sky_compress --in ' + self.template_file.format(**args)+'_'+inname+'.fits' + \
        ' --skyfilename ' + self.template_file.format(**args)+'_'+outname+'.fits' + \
        ' --blocksize 128'

        print '\n',cmd,'\n'    

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
        return    


#----------------------------------------#
#     this function runs immask          #
#----------------------------------------#
    def immask(self,CCD, inname,bkgname, outname, **args):
        args['ccd']=CCD


        cmd = 'immask all ' + self.template_file.format(**args)+'_'+inname+'.fits' +\
        ' ' +  self.template_file.format(**args)+'_'+outname+'.fits' +\
        '   --minSigma 6.0 --max_angle 75  --max_width 300 --nsig_detect 18 --nsig_mask 12 --nsig_merge 12'+\
        '   --nsig_sky 1.5  --min_fill 0.33 --min_DN 600  --nGrowCR 1 '+\
        ' --bkgfile '+self.template_file.format(**args)+'_'+bkgname+'.fits'+\
        ' --draw  --write_streaks  --streaksfile ' + self.template_file.format(**args)+'_streaksfile.fits' 

        print '\n',cmd,'\n'    

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
        os.remove(self.template_file.format(**args)+'_'+inname+'.fits')
        return    


#---------------------------------------#
#            skyCombine                 #
#---------------------------------------#

#PCFILENAME = PCFILENAMEPREFIX+'_'+str(year)+'_e1_'+FILTER+'_n04.fits'
#PCFILENAME = PCFILENAMEPREFIX+'_'+str(year)+'_'+str(epoch)+'_'+FILTER+'_n04.fits'

#copy_from_Dcache(correc_dir+'skytemp_'+str(year)+'_e1/'+PCFILENAME)
#copy_from_Dcache(correc_dir+'skytemp_'+str(year)+'_'+str(epoch)+'/'+PCFILENAME)


    def skyCombineFit(self,inputFile,skycombineFile,skyfitinfoFile,**args):

        self.link_from_Dcache(self.correc_dir+'/skypca/'+str(self.year)+str(self.epoch)+'/'+self.PCFILENAME.format(**args))
    #-----------------------
        cmd = 'ls  *'+inputFile+'.fits > listpcain'
        print'\n', cmd,'\n' 
        retval = subprocess.call(cmd, shell=True)

        if retval != 0:
            sys.exit(1)

        del cmd, retval

    #------------------------
        cmd = 'sky_combine --miniskylist listpcain -o ' + self.exp_template_file.format(**args)+'_'+skycombineFile+'.fits --ccdnums 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,62 --invalid S30,N30' 
        print cmd
        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)

        if retval != 0:
            sys.exit(1)

        del cmd, retval

    #----------------------
        cmd = 'sky_fit --infilename ' +  self.exp_template_file.format(**args)+'_'+ skycombineFile+'.fits' 
        cmd += ' --outfilename '+ self.exp_template_file.format(**args)+'_'+skyfitinfoFile+'.fits --pcfilename  '
        cmd += (self.PCFILENAME[self.PCFILENAME.rfind("/")+1:]).format(**args)
        print cmd
        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)

        if retval != 0:
            sys.exit(1)


#----------------------------------------#
#              starflat                  # 
#----------------------------------------#      

        

    def pixcorr_starflat(self,inname, outname, CCD,**args):
        args['ccd']=CCD
        self.link_from_Dcache(self.correc_dir+'/starflat/'+str(self.year)+str(self.epoch)+'/'+self.starflat.format(**args))

        
        cmd = 'pixcorrect_im --verbose --in ' +self.template_file.format(**args)+'_'+inname+'.fits' +\
        ' --starflat '+ self.starflat.format(**args) +\
        ' --out '+self.template_file.format(**args)+'_'+outname+'.fits'

        print '\n',cmd,'\n'

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)

        if retval != 0:
            sys.exit(1)

        os.remove(self.template_file.format(**args)+'_'+inname+'.fits')

#--------------------------------------------#
#                skysubtract                 #    
#-----------------------------------------  -#
#    pc_filename  =  ConfigSectionMap("skysubtract")['pcfilename']
#    weight = ConfigSectionMap("skysubtract")['weight']

    def skysubtract(self,CCD, inname, outname, skyfitinfoFile,**args ):
        args['ccd']=CCD
#
        self.link_from_Dcache(self.correc_dir+'/skypca/'+str(self.year)+str(self.epoch)+'/skytemp/'+self.pc_filename.format(**args))

#    copy_from_Dcache(correc_dir+'skytemp_'+str(year)+'_'+str(epoch)+'/'+pc_filename.format(**args))

        cmd = 'sky_subtract -i  '  +self.template_file.format(**args)+'_'+inname+'.fits'+\
        ' -o ' + self.template_file.format(**args)+'_'+outname+'.fits' +\
        ' --fitfilename '+ self.exp_template_file.format(**args)+'_'+skyfitinfoFile+'.fits ' +\
        ' --pcfilename ' +self.pc_filename.format(**args) +\
        ' --domefilename ' +self.flat.format(**args) +\
        ' --weight  ' +self.weight

        print '\n',cmd,'\n'
           
        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
        #   clean used files
        os.remove(self.template_file.format(**args)+'_'+inname+'.fits' )  
        os.remove(self.pc_filename.format(**args))
        os.remove(self.flat.format(**args))
        return


#------------------------------------------#
#              scamp                       #
#------------------------------------------#



    def scamp(self,inputFile):
        cmd = 'scamp ' + inputFile +\
        ' ' + '-AHEADER_GLOBAL ' + self.head_FILE +\
        ' -ASTRINSTRU_KEY DUMMY -AHEADER_SUFFIX .aheadnoexist -ASTREFMAG_LIMITS -99,17 ' +\
        ' -ASTREF_CATALOG ' +self.catalog_ref +' -c ' +self.default_scamp +\
        ' -WRITE_XML Y -XML_NAME scamp.xml -MOSAIC_TYPE SAME_CRVAL -ASTREF_BAND DEFAULT -POSITION_MAXERR 10.0 -NTHREADS 1 ' 

        print '\n',cmd,'\n'
        try:
            retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError, ex: # error code <> 0 
            print "--------error------"
            print ex.cmd
            print ex.message
            print ex.returncode
            print ex.output # contains stdout and stderr together 
            sys.exit(-1)
        return

#-----------------------------------------#
#           combineFiles                          
#----------------------------------------#-
    def combineFiles(self,inputFile, outputFile):
        cmd = ['fitscombine']
        file_list = glob.glob(inputFile)
        sort_list = sorted(file_list)
        for fileN in sort_list:
            cmd.append(fileN)
        cmd.append(outputFile)

        retval = subprocess.call(cmd)

        if retval != 0:
            sys.exit(1)
        return

#------------------------------------------#
#      clean files of given extension      #
#------------------------------------------#
    def fileclean(self,patt,ext):
        templ='*'+patt+ext
        file_list = glob.glob(templ)
        for fileN in file_list:
            os.remove(fileN)

#------------------------------------------#
#        sextractor                        #
#------------------------------------------#


    def sextractor(self, inname, outname, CCD, **args):
        args['ccd']=CCD

        cmd = 'sex ' + self.template_file.format(**args)+'_'+inname+'.fits[0]'+\
        ' -c  ' + self.configFile + ' -FILTER_NAME ' + self.sexconvFile + ' -STARNNW_NAME ' +self.sexnnwFile + ' -CATALOG_NAME  ' + self.template_file.format(**args)+'_'+outname+'.fits'+\
        ' -FLAG_IMAGE '  + self.template_file.format(**args)+'_'+inname+'.fits[1] -PARAMETERS_NAME ' + self.sexparamFile +\
        ' -DETECT_THRESH 10.0 -SATUR_KEY SATURATE  -CATALOG_TYPE FITS_LDAC -WEIGHT_IMAGE  ' + self.template_file.format(**args)+'_'+inname+'.fits[2]'+\
        '  -WEIGHT_TYPE  MAP_WEIGHT  '

        print '\n',cmd,'\n'

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
        return
    
#------------------------------------------#
#        sextractor for PSFEX              #
#------------------------------------------#


    def sextractorPSFEX(self, inname, outname, CCD, **args):
        args['ccd']=CCD

        cmd = 'sex ' + self.template_file.format(**args)+'_'+inname+'.fits[0]'+\
        ' -c  ' + self.confPSF + ' -FILTER_NAME ' + self.sexconvFile + ' -STARNNW_NAME ' +self.sexnnwFile + ' -CATALOG_NAME  ' + self.template_file.format(**args)+'_'+outname+'.fits'+\
        ' -FLAG_IMAGE '  + self.template_file.format(**args)+'_'+inname+'.fits[1] -PARAMETERS_NAME ' +self.sexparamPSFEXFile  +\
        '  -SATUR_KEY SATURATE  -CATALOG_TYPE FITS_LDAC -WEIGHT_IMAGE  ' + self.template_file.format(**args)+'_'+inname+'.fits[2]'+\
        '  -WEIGHT_TYPE  MAP_WEIGHT  '

        print '\n',cmd,'\n'

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
        return

#-----------------------------------#
#  nulling weights for sky ccd31    #
#-----------------------------------#

    def nullweightbkg(self, inname, outname,CCD,**args):
        args['ccd']=CCD
    # Preprocess the image for SExtractor by nulling weights in bad regions
        cmd = 'null_weights -i ' + self.template_file.format(**args)+'_'+inname+'.fits' \
          ' -o ' + self.template_file.format(**args)+'_'+outname+'.fits' + \
          ' --null_mask BADAMP,EDGEBLEED,EDGE,STAR,TRAIL' 

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)

#---------------------------------------------
#    sextractorsky bkg calculator
#---------------------------------------------



    def sextractorsky(self, inname, outname, CCD, **args):
        args['ccd']=CCD

        cmd = 'sex ' + self.template_file.format(**args)+'_'+inname+'.fits[0]'+\
        ' -c  ' + self.configFile +' -CHECKIMAGE_TYPE BACKGROUND ' +\
        ' -DETECT_THRESH 1000 -FILTER N ' +\
        ' -CHECKIMAGE_NAME ' + self.template_file.format(**args)+'_'+outname+'.fits'+\
        ' -WEIGHT_TYPE MAP_WEIGHT -WEIGHT_IMAGE '+self.template_file.format(**args)+'_'+inname+'.fits[2],'+self.template_file.format(**args)+'_'+inname+'.fits[2]' +\
        ' -PARAMETERS_NAME ' + self.sexbkgparamFile +\
        ' -CATALOG_TYPE NONE -INTERP_TYPE ALL -INTERP_MAXXLAG 16 -INTERP_MAXYLAG 16 '

        print '\n',cmd,'\n'
        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
        return

#--------------------------------------------#
#          psfex                             #
#--------------------------------------------#

    def psfex(self, name,CCD, **args):
        args['ccd']=CCD

        cmd = 'psfex ' + self.template_file.format(**args)+'_'+name+'.fits -c  ' +self.config_filePSF +\
        '  -OUTCAT_NAME  '+ self.template_file.format(**args)+'_psflist.fits  -OUTCAT_TYPE FITS_LDAC'
        print '\n',cmd,'\n'

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)

        return


#---------------------------------------------#
#    sextractor with PSF                      #
#---------------------------------------------#

    def sextractorPSF(self, name, name1, outname, filepsf, CCD, **args):
        args['ccd']=CCD

        h=DESImage.load(self.template_file.format(**args)+'_'+str(name)+'.fits')
        fwhm = 0.263*float(h.header['FWHM'])


        cmd = 'sex ' + self.template_file.format(**args)+'_'+name+'.fits[0]'+\
        ' -PSF_NAME ' + self.template_file.format(**args)+'_'+filepsf+\
        ' -c  ' + self.configFile2 + ' -FILTER_NAME ' + self.sexconvFile2 + ' -STARNNW_NAME ' +self.sexnnwFile + ' -CATALOG_NAME  ' + self.template_file.format(**args)+'_'+outname+'.fits'+\
        ' -FLAG_IMAGE '  + self.template_file.format(**args)+'_'+name+'.fits[1] -PARAMETERS_NAME ' + self.sexparamFile_2 +\
        ' -INTERP_TYPE VAR_ONLY  -INTERP_MAXXLAG 4 -INTERP_MAXYLAG 4 -SEEING_FWHM ' + str(fwhm) +\
        ' -DETECT_THRESH 1.5 -SATUR_KEY SATURATE  -CATALOG_TYPE FITS_LDAC '+\
        ' -WEIGHT_IMAGE '+self.template_file.format(**args)+'_'+name1+'.fits[2],'+self.template_file.format(**args)+'_'+name1+'.fits[2]'+ \
        '  -WEIGHT_TYPE MAP_WEIGHT  -CHECKIMAGE_NAME ' + self.template_file.format(**args)+'_segmap.fits -CHECKIMAGE_TYPE SEGMENTATION '
        print ' sextractorPSF \n',cmd,'\n'
#        ' -INTERP_TYPE NONE  -SEEING_FWHM ' + str(fwhm) +\

        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
        os.remove(self.template_file.format(**args)+'_'+name+'.fits')
        return

#--------------------------------------#
#     fwhm routine from Wrappers      #
#--------------------------------------#

    debug = 0
    def fwhm(self, incat ,debug):
        debug = 0
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

    def change_head(self, FileN, catalog, image, outname, CCD, **args):

        ccdLen = len(CCD)

        #Getting the data and saving into an array
        o = open(FileN,'r').read().splitlines()
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
            catalog1 = self.template_file.format(**args)+'_'+catalog+'.fits'
            image1 = self.template_file.format(**args)+'_'+image+'.fits'

            (fwhm_, ellip, count) = self.fwhm(catalog1,0)

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

                h.save(self.template_file.format(**args)+'_'+outname+'.fits')

#-----------------------------------#
#  nulling weights in bad regions   #
#-----------------------------------#

    def rowinterp_nullweight(self, inname, outname,CCD,**args):
        args['ccd']=CCD
        # Preprocess the image for SExtractor by interpolating (temporarily) across bleedtrails
        cmd = 'rowinterp_nullweight -i ' + self.template_file.format(**args)+'_'+inname+'.fits' \
            ' -o ' + self.template_file.format(**args)+'_'+outname+'.fits' + \
            ' -l ' + self.template_file.format(**args)+'_'+outname+'.log' + \
            ' --interp_mask TRAIL --invalid_mask EDGE --max_cols 50 -vv --null_mask ' + \
            ' BADAMP,EDGEBLEED,EDGE,CRAY'
        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)


#-----------------------------------#
#  nulling weights in bad regions   #
#-----------------------------------#

    def nullweight(self, inname, outname,CCD,**args):
        args['ccd']=CCD
        # Preprocess the image for SExtractor by nulling weights in bad regions
        cmd = 'null_weights -i ' + self.template_file.format(**args)+'_'+inname+'.fits' \
            ' -o ' + self.template_file.format(**args)+'_'+outname+'.fits' + \
            ' -l ' + self.template_file.format(**args)+'_'+outname+'.log' + \
            ' --null_mask BADAMP,EDGEBLEED,EDGE' + \
            ' --resaturate'
        retval = subprocess.call(cmd.split(),stderr=subprocess.STDOUT)
        if retval != 0:
            sys.exit(1)
            
#--------------------------------#
#      creating regions          #
#--------------------------------#

    def read_geometry(self, inname, outname, CCD, **args):

        args['ccd']=CCD    
        fileN = self.template_file.format(**args)+'_'+inname+'.fits'

        cat = pyfits.open(fileN)
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
        np.savetxt(self.template_file.format(**args)+'_'+outname+'.reg', DAT, delimiter=" ", fmt="%s")   

    def sanityCheck(self,fName):
        f = pyfits.open(fName)
        nstar = f[2].header['NAXIS2']
        f.close()
        if nstar < 5:
            raise ValueError('A very low number of stars')
        return nstar
        
    def setArgs(self,**args):
        self.inargs = args

    def l1p(self,ccdstring):
        args = self.inargs.copy()
        self.pixcorrect('detrend',ccdstring,**args)
        self.nullweight('detrend', 'nullweight', ccdstring,**args)
        self.sextractor('nullweight', 'sextractor', ccdstring,**args )
        
    def l2p(self,ccdstring):
        self.bleedmask(ccdstring,'wcs','bleedmasked',**self.inargs)
        self.skycompress(ccdstring,'bleedmasked','bleedmask-mini',**self.inargs)


        
    def l3p(self,ccdstring):
        self.skysubtract(ccdstring,'bleedmasked','skysub','skyfit-binned-fp',**self.inargs )
        self.pixcorr_starflat('skysub', 'starflat', ccdstring,**self.inargs)
        self.nullweightbkg('starflat','nullwtbkg',ccdstring,**self.inargs)
        self.sextractorsky('nullwtbkg','bkg',ccdstring,**self.inargs)
        self.immask(ccdstring,'starflat','bkg','immask',**self.inargs) 
        self.rowinterp_nullweight('immask', 'nullweightimmask', ccdstring,**self.inargs)
        self.sextractorPSFEX('nullweightimmask', 'sextractorPSFEX', ccdstring,**self.inargs )
#
        self.psfex( 'sextractorPSFEX', ccdstring,**self.inargs)
        self.sextractorPSF('nullweightimmask', 'nullweightimmask', 'fullcat', 'sextractorPSFEX.psf', ccdstring,**self.inargs)    
        print "done with Sextractor for CCD %s \n" % ccdstring
        
    " unpack scamp log to select resolution and number of stars "
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
#

    
        
if __name__ == '__main__':
    nbpar = len(sys.argv)
    print sys.argv
    conF = "confFile"
    conF = sys.argv[1]
    multiprocessing.freeze_support()
    print " Start SEProc.py with conf File %s\n" % conF
    se = SEProcPoolST(conF)
    print "instance created \n"
    EXPNUM =  se.ConfigSectionMap("General")['expnum']
    FILTER = se.ConfigSectionMap("General")['filter']
    NITE = se.ConfigSectionMap("General")['nite']
    data_dir = se.ConfigSectionMap("General")['data_dir']
    data_file = se.ConfigSectionMap("General")['data_file']
    CCD = (se.ConfigSectionMap("General")['chiplist']).split( ',')
    rRun = str(se.ConfigSectionMap("General")['r'])
    pRun = str(se.ConfigSectionMap("General")['p'])
    YEAR = se.ConfigSectionMap("General")['year']
    EPOCH = se.ConfigSectionMap("General")['epoch']
    ncpu = int(se.ConfigSectionMap("General")['ncpu'])
    print "ncpu=%d \n" % ncpu
#EXPFILE =  'DECam_00'+str(EXPNUM)+'.fits.fz'
    EXPFILE = data_file
    print "Start working with file %s \n" % EXPFILE
    args = {'expnum': EXPNUM, 'filter': FILTER, 'ccd':'0', 'r':rRun, 'p':pRun, 'year': YEAR, 'epoch': EPOCH}
    se.setArgs(**args)
#running crosstalk
# parameters are crosstalk(EXPFILE,NITE,**args)
# I have to replace NITE with empty string to avoid modification of the file path
# as it add to the path NITE and we have different data model
#
    print " Start crosstalk \n"
    se.crosstalk(EXPFILE,'',**args)
    nccd = len(CCD)
    xfiles_list = glob.glob('*_xtalk.fits')
    if len(xfiles_list) <  nccd:
        print " Possibly corrupted file expect %d extensions but got only %d \n" % (nccd,len(xfiles_list))
        sys.exit(-1)
#running pixelcorrect and bleedmask
    se.link_from_Dcache(se.data_conf+'default.psf')

    
    instrings = []
    for ccd in CCD:
        ccdstring= "%02d"%int(ccd) 
        instrings.append(ccdstring)

    
    
    pool = Pool(ncpu)
    pars = [(se, ccdstring) for ccdstring in instrings ]
    pool.map(runL1P, pars)
    se.fileclean('xtalk','.fits') 
    se.fileclean('nullweight','.fits')
    
    se.combineFiles('D'+("%08d" % int(EXPNUM))+'**sextractor.fits', 'Scamp_allCCD_r'+rRun+'p'+pRun+'.fits')
    try:
        se.sanityCheck('D'+("%08d" % int(EXPNUM))+'_'+FILTER+'_01'+'_r'+rRun+'p'+pRun+'_sextractor.fits')
    except ValueError as err:
        print(err.args)
        sys.exit(-1)
    se.fileclean('bpm','.fits')
    se.fileclean('biascor','.fits')
        
    se.scamp('Scamp_allCCD_r'+rRun+'p'+pRun+'.fits')
    se.change_head('Scamp_allCCD_r'+rRun+'p'+pRun+'.head', 'sextractor', 'detrend', 'wcs', CCD, **args)
#

    print "SECOND LOOP \n"
    pool.map(runL2P, pars)
    se.fileclean('detrend','.fits')

    se.skyCombineFit('bleedmask-mini','bleedmask-mini-fp','skyfit-binned-fp',**args)
# 

    print "THIRD LOOP \n"
    pool.map(runL3P, pars)
#
    pool.close()
    pool.join()
    se.fileclean('nullwtbkg','.fits')
#    print res
    

    print " End run on exposure  \n" 
