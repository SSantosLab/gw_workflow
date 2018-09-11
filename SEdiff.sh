#!/bin/bash

if [ $# -lt 1 ]; then
    echo "usage: SEdiff.sh -E EXPNUM -r RNUM -p PNUM -n NITE -b BAND (i|r|g|Y|z|u) -S season (dpXX) [-c ccdlist] [-d destcache (scratch|persistent)] [-m SCHEMA (gw|wsdiff)] [-v diffimg_version] [-t] [-C] [-j] [-s] [-O] [-V SNVETO_NAME] [-T STARCAT_NAME] [-Y] [-F]" 
    exit 1
fi

OLDHOME=$HOME
export HOME=$PWD
DESTCACHE="persistent"
SCHEMA="gw" 
DIFFIMG_VERSION="gwdevel13" # can change this with parameter -v <diffimg_version>
ulimit -a
OVERWRITE=false
SKIPSE=false
CCDNUM_LIST=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62
IFDHCP_OPT=""
DOCALIB="false"
FAILEDEXPS=""
FULLCOPY=false

umask 002 

#######################################
###  Protection against dead nodes  ###
if [ ! -f /cvmfs/fermilab.opensciencegrid.org/products/common/etc/setup ]; then
   echo "Unable to find fermilab CVMFS repo setup file, so I assume the whole repo is missing."
   echo "I will sleep for four hours to block the slot and then exit with an error."
   sleep 14400
   exit 1
fi

if [ ! -f /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh ]; then
   echo "Unable to find DES CVMFS repo startup file, so I assume the whole repo is missing."
   echo "I will sleep for four hours to block the slot and then exit with an error."
   sleep 14400
   exit 1
fi
#######################################

### Protection against wrong StashCache version ###
if [ -n "${MIN_STASH_VERSION}" ]; then
    echo "MIN_STASH_VERSION = $MIN_STASH_VERSION"
    while [ $MIN_STASH_VERSION -gt $(attr -q -g revision /cvmfs/des.osgstorage.org) ]
    do
    echo "Revision of /cvmfs/des.osgstorage.org below minimum value of ${MIN_STASH_VER}. Sleeping 15 minutes to check next update."
    sleep 900
    done
else 
    echo "$MIN_STASH_VERSION not set. Continuing." 
fi

####################

# get some worker node information
echo "Worker node information: `uname -a`"

OLDHOME=$HOME

export HOME=$PWD

### Force use of SLF6 versions for systems with 3.x kernels
case `uname -r` in
    3.*) export UPS_OVERRIDE="-H Linux64bit+2.6-2.12";;
    4.*) export UPS_OVERRIDE="-H Linux64bit+2.6-2.12";;
esac

# check that xrdcp and uberftp are installed
which xrdcp >/dev/null 2>&1
CHECK_XRDCP=$?
which uberftp >/dev/null 2>&1
CHECK_UBERFTP=$?

# pretend that CHECK_XRDCP failed if we detect version 4.6.0 (or 4.7.0) since it is buggy
XRDCP_VERSION=`xrdcp --version 2>&1`
if [[ $XRDCP_V == *4.6.0* ]] || [[ $XRDCP_VERSION == *4.7.0* ]] ; then CHECK_XRDCP=1 ; fi

if [ $CHECK_XRDCP -ne 0 ] || [ $CHECK_UBERFTP -ne 0 ]; then
    if [ -f /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.3/current/el6-x86_64/setup.sh ]; then
	. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.3/current/el6-x86_64/setup.sh
    # TODO from RUN_DIFFIMG_PIPELINE
    #if [ -f /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/current/el6-x86_64/setup.sh ]; then
    #. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/current/el6-x86_64/setup.sh
    else
    "Cannot find OASIS CVMFS setup file, and xrdcp and/or uberftp are not in the path."
    fi
fi

# . /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.3/3.3.27/el6-x86_64/setup.sh
# 2018-03-09 replace with current OSG stack and 3.4 
# from verifySE.sh TODO
. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.4/current/el6-x86_64/setup.sh
echo "xrdcp version and path now $(xrdcp --version 2>&1) and $(which xrdcp 2>&1)"


# set environment
source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh
export IFDH_CP_MAXRETRIES=2
export IFDH_XROOTD_EXTRA="-f -N"
# export IFDH_XROOTD_EXTRA="-S 4 -f -N" #TODO from verifySE.sh
export XRD_REDIRECTLIMIT=255
export IFDH_CP_UNLINK_ON_ERROR=1
#for IFDH
export EXPERIMENT=des
export PATH=${PATH}:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/cpn/v1_7/NULL/bin:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/ifdhc/v2_1_0/Linux64bit-2-6-2-12/bin
export PYTHONPATH=/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/ifdhc/v2_1_0/Linux64bit-2-6-2-12/lib/python:${PYTHONPATH}
export IFDH_NO_PROXY=1
export IFDHC_LIB=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v2_1_0/Linux64bit-2-6-2-12/lib
export IFDH_GRIDFTP_EXTRA="-st 1800"
export XRD_REQUESTTIMEOUT=1200
export IFDHC_CONFIG_DIR=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc_config/v2_1_0/NULL

export CONDA_DIR=/cvmfs/des.opensciencegrid.org/fnal/anaconda2
source $CONDA_DIR/etc/profile.d/conda.sh
conda activate des18a

# parse arguments and flags
ARGS="$@"
##### Don't forget to shift the args after you pull these out #####
while getopts "E:n:b:r:p:S:d:c:v:V:T:F:CjhsYOtm:" opt $ARGS
do case $opt in
    E)
            [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: exposure number must be an integer! You put $OPTARG" ; exit 1; }
            EXPNUM=$OPTARG #TODO export?
            shift 2
            ;;
    n)
            [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: Night must be an integer! You put $OPTARG" ; exit 1; }
            NITE=$OPTARG #TODO export?
            shift 2
            ;;
    b)
            case $OPTARG in
                i|r|g|Y|z|u)
                    BAND=$OPTARG
                    shift 2
                    ;;
                *)
                    echo "Error: band option must be one of r,i,g,Y,z,u. You put $OPTARG."
                    exit 1
                    ;;
            esac
            ;;
    r)
            RNUM=$OPTARG #TODO export?
            shift 2
            ;;
    p)
            PNUM=$OPTARG #TODO export?
            shift 2
            ;;
    S)
            procnum=$OPTARG
            shift 2
            ;;
    j)
            JUMPTOEXPCALIB=true
            shift 
            ;;
    t)
            TEMPLATE=true
            shift
            ;;
    V)
            SNVETO_NAME=$OPTARG
            shift 2
            ;;
    T)
            STARCAT_NAME=$OPTARG
            shift 2
            ;;
    C)
            DOCALIB=true
            shift 
            ;;
    s)
            SINGLETHREAD=true
            shift 
            ;;
    d) # add some checks here?
	    DESTCACHE=$OPTARG
	    shift 2
	    ;;
    m)
	    SCHEMA=$OPTARG
	    shift 2
	    ;;
    v)
        DIFFIMG_VERSION=$OPTARG
        shift 2

        ;;
    F)
        FULLCOPY=true
        shift
        ;;
    Y)
            SPECIALY4=true
            shift 
            ;;
    O)
	    OVERWRITE=true
	    shift
	    ;;
    h)
        echo "usage: SEdiff.sh -E EXPNUM -r RNUM -p PNUM -n NITE -b BAND (i|r|g|Y|z|u) -S season (dpXX) [-c ccdlist] [-d destcache (scratch|persistent)] [-m SCHEMA (gw|wsdiff)] [-v diffimg_version] [-t] [-C] [-j] [-s] [-O] [-V SNVETO_NAME] [-T STARCAT_NAME] [-Y] [-F]" 
	    exit 1
            ;;
    c)
    #TODO: does this work for a comma-separated list?
        [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: CCD number must be an integer! You put $OPTARG" ; exit 1; }
        [[ $OPTARG -lt 70 ]] || { echo "Error: the chip number must be less than 70. You entered $OPTARG." ; exit 1; }  
        CCDNUM_LIST=$OPTARG
        shift 2

        ;;
    :)
            echo "Option -$OPTARG requires an argument."
            exit 1
            ;;
esac
done

if [ "x$EXPNUM" = "x" ]; then echo "Exposure number not set; exiting." ; exit 1 ; fi
if [ "x$NITE"   = "x" ]; then echo "NITE not set; exiting."            ; exit 1 ; fi
if [ "x$BAND"   = "x" ]; then echo "BAND not set; exiting."            ; exit 1 ; fi
if [ "x$RNUM"   = "x" ]; then echo "r number not set; exiting."        ; exit 1 ; fi
if [ "x$PNUM"   = "x" ]; then echo "p number not set; exiting."        ; exit 1 ; fi
if [ "x$procnum" == "x" ]; then echo "season number not set (use -S option); exiting." ; exit 1 ; fi
if [ "x$CCDNUM_LIST" == "x" ]; then echo "CCD number not set; exiting."; exit 1 ; fi
rpnum="r${RNUM}p${PNUM}"
# tokenize ccd argument (in case of multiple comma-separated ccds)
ccdlist=(${CCDNUM_LIST//,/ })

# get filenames
immaskfiles="$(ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/D$(printf %08d ${EXPNUM})_${BAND}_$(printf %02d ${CCDNUM_LIST})_r${RNUM}p${PNUM}_immask.fits.fz | grep -v "/$")"
nimmask=`echo $immaskfiles | wc -w`
if [ $nimmask -ge 1 ]; then
    psffiles="$(ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/D$(printf %08d ${EXPNUM})_${BAND}_$(printf %02d ${CCDNUM_LIST})_r${RNUM}p${PNUM}_fullcat.fits | grep -v "/$")" 
    npsf=`echo $psffiles | wc -w`
    if [ $npsf -ge 1 ] || [ "$DOCALIB" == "false" ]; then
	csvfiles="`ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/allZP_D$(printf %08d ${EXPNUM})_r${RNUM}p${PNUM}.csv | grep -v "/$"` `ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/Zero_D$(printf %08d ${EXPNUM})_r${RNUM}p${PNUM}.csv | grep -v "/$"` `ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/D$(printf %08d ${EXPNUM})_r${RNUM}p${PNUM}_ZP.csv | grep -v "/$"`" 
	ncsv=`echo $csvfiles | wc -w`
	if [ $ncsv -ge 3 ] || [ "$DOCALIB" == "false" ]; then
	    if [ "$OVERWRITE" == "false" ]; then
		echo "All SE processing for $EXPNUM, r=$RNUM, p=$PNUM is complete. We will skip the SE step."
		SKIPSE=true
	    else
		echo "All files are present, but the -O option was given, so we are continuing on with the job."
	    fi
	else
	    # we don't have the combined csv files for this exposure and rpnum. Let's check for the CCD-specific files now. 
	    # If they are present, 
	    ccdcsvfiles="`ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/allZP_D$(printf %08d ${tempexp})_r${RNUM}p${PNUM}.csv | grep fnal` `ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/Zero_D$(printf %08d ${tempexp})_$(printf %02d $CCDNUM_LIST)_r${RNUM}p${PNUM}.csv | grep fnal` `ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/D$(printf %08d ${tempexp})_$(printf %02d $CCDNUM_LIST)_r${RNUM}p${PNUM}_ZP.csv | grep fnal`" 
	    nccdcsv=`echo $ccdcsvfiles | wc -w`
	    if [ $nccdcsv -ge 2 ]; then
		if [ "$OVERWRITE" == "false" ]; then
		    echo "All SE processing for $EXPNUM, r=$RNUM, p=$PNUM is complete. We will skip the SE step."
		    SKIPSE=true
		else
		    echo "All files are present, but the -O option was given, so we are continuing on with the job."
		fi
	    else
	    	echo "csv files not all present; continuing with the job."
	    fi
	fi
    else
	echo "fullcat files not all present; continuing with the job."
    fi
else
    echo "immask files not all present; continuing with the job."
fi

# do SE processing
if [ "$SKIPSE" == "false" ] ; then # if statement allows SE to be skipped if SE is complete and the -O flag is not used
    echo "***** BEGIN SE PROCESSING *****"
    # flag for whether or not to do calibration (if the image isn't/is in the DES footprint)
    if [ "$DOCALIB" == "true" ] ; then
        echo "This SE job will include BLISS calib step."
    else
        echo "This SE job will use calib info from the DB."
        filestorm="`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_r'${RNUM}p${PNUM}'_fullcat.fits' | grep fits | grep fnal` `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_r'${RNUM}p${PNUM}'*csv*' | grep csv | grep fnal`"
        if [ ! -z "${filestorm}" ]; then
            ifdh rm $filestorm
        fi
    fi
    
    #### add other code here from Nikolay's area
    #TODO move all the necessary files into the tar.gz and into /pnfs/ so they don't have to be copied in here
    #ifdh cp -D /pnfs/des/persistent/desdm/code/desdmLiby1e2.py /pnfs/des/persistent/desdm/code/run_desdmy1e2.py /pnfs/des/persistent/desdm/code/run_SEproc.py  /pnfs/des/persistent/desdm/code/getcorners.sh /pnfs/des/persistent/kuropat/scripts/MySoft.tgz  /pnfs/des/scratch/gw/code/test_mysql_libs.tar.gz /pnfs/des/scratch/nglaeser/BLISS-expCalib_Y3apass-old.py /pnfs/des/scratch/nglaeser/Scamp_allCCD_r4p5.fits ./ || { echo "Error copying input files. Exiting." ; exit 2 ; }
    #ifdh cp -D /pnfs/des/resilient/gw/code/MySoft3.tar.gz  /pnfs/des/scratch/gw/code/test_mysql_libs.tar.gz /pnfs/des/scratch/nglaeser/Scamp_allCCD_r4p5.fits ./ || { echo "Error copying input files. Exiting." ; exit 2 ; }
    ifdh cp -D /pnfs/des/resilient/gw/code/MySoft4.tar.gz  /pnfs/des/resilient/gw/code/test_mysql_libs.tar.gz ./ || { echo "Error copying input files. Exiting." ; exit 2 ; }
    tar xzf ./MySoft4.tar.gz
    
    ifdh cp --force=xrootd /pnfs/des/persistent/${SCHEMA}/db-tools/desservices.ini ${HOME}/.desservices.ini
    
    tar xzfm ./test_mysql_libs.tar.gz
    
    
#    ifdh cp -D /pnfs/des/scratch/nglaeser/BLISS-expCalib_Y3apass-old.py ./ || { echo "Error copying BLISS-old.py file. Exiting." ; exit 2; }
#    ifdh cp -D /pnfs/des/scratch/nglaeser/BLISS-expCalib_Y3apass.py ./ || { echo "Error copying BLISS.py file. Exiting." ; exit 2; }
#    ifdh cp -D /pnfs/des/scratch/nglaeser/run_SEproc.py /pnfs/des/scratch/nglaeser/run_desdmy1e2.py /pnfs/des/scratch/nglaeser/desdmLiby1e2.py ./ || { echo "Error copying run_SEproc.py and run_desdmy1e2.py. Exiting." ; exit 2; }
#    ifdh cp -D /pnfs/des/scratch/nglaeser/desdmLiby1e2.py ./ || { echo "Error copying desdmLiby1e2.py. Exiting." ; exit 2; }
#    ifdh cp -D /pnfs/des/scratch/nglaeser/make_red_catlist.py ./ || { echo "Error copying make_red_catlist.py Exiting." ; exit 2; }
    
    export DES_SERVICES=${HOME}/.desservices.ini
    chmod 600 ${HOME}/.desservices.ini
    chmod +x make_red_catlist.py BLISS-expCalib_Y3apass.py BLISS-expCalib_Y3apass-old.py getcorners.sh
    
    rm -f confFile
    
    # Automatically determine year and epoch based on exposure number
    #  NAME MINNITE  MAXNITE   MINEXPNUM  MAXEXPNUM
    # -------------------------------- -------- -------- ---------- ----------
    # SVE1 20120911 20121228 133757 164457
    # SVE2 20130104 20130228 165290 182695
    # Y1E1 20130815 20131128 226353 258564
    # Y1E2 20131129 20140209 258621 284018
    # Y2E1 20140807 20141129 345031 382973
    # Y2E2 20141205 20150518 383751 438346
    # Y3   20150731 20160212 459984 516846
    #######################################333
    
    
    # need to implement here handling of different "epochs" within the same year

    # need lso the "if not special option to use Y4E1 numbers"
    # note that we could use {filter:s}no61.head for sve1, sve2, and y1e1, but for consistency 
    # we are doing no2no61.head for everything as of now (2017-01-04)

    # IMPORTANT NOTE: Be sure that all of the filenames below are in SINGLE QUOTES.
    if [ "${SPECIALY4}" == "true" ]; then
        
        YEAR=y4
        EPOCH=e1
        biasfile='D_n20151113t1123_c{ccd:>02s}_r2350p02_biascor.fits'
        bpmfile='D_n20151113t1123_c{ccd:>02s}_r2400p01_bpm.fits'
        dflatfile='D_n20151113t1123_{filter:s}_c{ccd:>02s}_r2350p02_norm-dflatcor.fits'
        skytempfile='Y2T4_20150715t0315_{filter:s}_c{ccd:>02s}_r2404p01_skypca-tmpl.fits'
        starflatfile='Y2A1_20150715t0315_{filter:s}_c{ccd:>02s}_r2360p01_starflat.fits'
        headfile='f'$CCDNUM_LIST'.head'
        pcaprefix='binned-fp/Y2T4_20150715t0315_{filter:s}_r2404p01_skypca-binned-fp.fits'
    else
        if [ $EXPNUM -lt 165290 ]; then
        YEAR=sv
        EPOCH=e1
        biasfile='D_n20130115t0131_c{ccd:>02s}_r1788p01_biascor.fits'
        bpmfile='D_n20130115t0130_c{ccd:>02s}_r1975p01_bpm.fits'
        dflatfile='D_n20130115t0131_{filter:s}_c{ccd:>02s}_r1788p01_norm-dflatcor.fits'
        skytempfile='Y2A1_20130101t0315_{filter:s}_c{ccd:>02s}_r1979p01_skypca-tmpl.fits'
        starflatfile='Y2A1_20130101t0315_{filter:s}_c{ccd:>02s}_r1976p01_starflat.fits'
        headfile='f'$CCDNUM_LIST'.head'
        pcaprefix='binned-fp/Y2A1_20130101t0315_{filter:s}_r1979p01_skypca-binned-fp.fits'	
        elif [ $EXPNUM -lt 226353 ]; then
        YEAR=sv
        EPOCH=e1
        biasfile='D_n20130115t0131_c{ccd:>02s}_r1788p01_biascor.fits'
        bpmfile='D_n20130115t0130_c{ccd:>02s}_r1975p01_bpm.fits'
        dflatfile='D_n20130115t0131_{filter:s}_c{ccd:>02s}_r1788p01_norm-dflatcor.fits'
        skytempfile='Y2A1_20130101t0315_{filter:s}_c{ccd:>02s}_r1979p01_skypca-tmpl.fits'
        starflatfile='Y2A1_20130101t0315_{filter:s}_c{ccd:>02s}_r1976p01_starflat.fits'
        headfile='f'$CCDNUM_LIST'.head'
        pcaprefix='binned-fp/Y2A1_20130101t0315_{filter:s}_r1979p01_skypca-binned-fp.fits'	
        elif [ $EXPNUM -lt 258564 ]; then
        YEAR=y1
        EPOCH=e1
        biasfile='D_n20130916t0926_c{ccd:>02s}_r1999p06_biascor.fits'
        bpmfile='D_n20130916t0926_c{ccd:>02s}_r2083p01_bpm.fits'
        dflatfile='D_n20130916t0926_{filter:s}_c{ccd:>02s}_r1999p06_norm-dflatcor.fits'
        #skytempfile='Y2A1_20131129t0315_{filter:s}_c{ccd:>02s}_r2106p01_skypca-tmpl.fits'
        skytempfile='Y2A1_20130801t1128_{filter:s}_c{ccd:>02s}_r2044p01_skypca-tmpl.fits'
        starflatfile='Y2A1_20130801t1128_{filter:s}_c{ccd:>02s}_r2046p01_starflat.fits'
        headfile='f'$CCDNUM_LIST'.head'
        pcaprefix='binned-fp/Y2A1_20130801t1128_{filter:s}_r2044p01_skypca-binned-fp.fits'
        elif [ $EXPNUM -lt 284391 ]; then
        YEAR=y1
        EPOCH=e2
        biasfile='D_n20140117t0129_c{ccd:>02s}_r2045p01_biascor.fits'
        bpmfile='D_n20140117t0129_c{ccd:>02s}_r2105p01_bpm.fits'
        dflatfile='D_n20140117t0129_{filter:s}_c{ccd:>02s}_r2045p01_norm-dflatcor.fits'
        skytempfile='Y2A1_20131129t0315_{filter:s}_c{ccd:>02s}_r2106p01_skypca-tmpl.fits'
        starflatfile='Y2A1_20131129t0315_{filter:s}_c{ccd:>02s}_r2107p01_starflat.fits'
        headfile='f'$CCDNUM_LIST'.head'
        pcaprefix='binned-fp/Y2A1_20131129t0315_{filter:s}_r2106p01_skypca-binned-fp.fits'
        elif [ $EXPNUM -le 383321 ]; then
        YEAR=y2
        EPOCH=e1
        biasfile='D_n20141204t1209_c{ccd:>02s}_r1426p08_biascor.fits'
        bpmfile='D_n20141020t1030_c{ccd:>02s}_r1474p01_bpm.fits'
        dflatfile='D_n20141020t1030_{filter:s}_c{ccd:>02s}_r1471p01_norm-dflatcor.fits'
        skytempfile='Y2A1_20140801t1130_{filter:s}_c{ccd:>02s}_r1635p01_skypca-tmpl.fits'
        starflatfile='Y2A1_20140801t1130_{filter:s}_c{ccd:>02s}_r1637p01_starflat.fits'
        headfile='f'$CCDNUM_LIST'.head'                                                 
        pcaprefix='binned-fp/Y2A1_20140801t1130_{filter:s}_r1635p01_skypca-binned-fp.fits'
        elif [ $EXPNUM -le 438444 ]; then
        YEAR=y2
        EPOCH=e2
        biasfile='D_n20150105t0115_c{ccd:>02s}_r2050p02_biascor.fits'
        bpmfile='D_n20150105t0115_c{ccd:>02s}_r2134p01_bpm.fits'
        dflatfile='D_n20150105t0115_{filter:s}_c{ccd:>02s}_r2050p02_norm-dflatcor.fits'
        skytempfile='Y2A1_20141205t0315_{filter:s}_c{ccd:>02s}_r2133p01_skypca-tmpl.fits'
        starflatfile='Y2A1_20141205t0315_{filter:s}_c{ccd:>02s}_r2132p01_starflat.fits'
        headfile='f'$CCDNUM_LIST'.head'                                                 
        pcaprefix='binned-fp/Y2A1_20141205t0315_{filter:s}_r2133p01_skypca-binned-fp.fits'
        elif [ $EXPNUM -le 519543 ]; then
        YEAR=y3
        EPOCH=e1
        biasfile='D_n20151113t1123_c{ccd:>02s}_r2350p02_biascor.fits'
        bpmfile='D_n20151113t1123_c{ccd:>02s}_r2359p01_bpm.fits'
        dflatfile='D_n20151113t1123_{filter:s}_c{ccd:>02s}_r2350p02_norm-dflatcor.fits'
        skytempfile='Y2A1_20150715t0315_{filter:s}_c{ccd:>02s}_r2361p01_skypca-tmpl.fits'
        starflatfile='Y2A1_20150715t0315_{filter:s}_c{ccd:>02s}_r2360p01_starflat.fits'
        headfile='f'$CCDNUM_LIST'.head'                                                 
        pcaprefix='binned-fp/Y2A1_20150715t0315_{filter:s}_r2361p01_skypca-binned-fp.fits'
        else
        YEAR=y4
        EPOCH=e1
        biasfile='D_n20151113t1123_c{ccd:>02s}_r2350p02_biascor.fits'
        bpmfile='D_n20151113t1123_c{ccd:>02s}_r2400p01_bpm.fits'
        dflatfile='D_n20151113t1123_{filter:s}_c{ccd:>02s}_r2350p02_norm-dflatcor.fits'
        skytempfile='Y2T4_20150715t0315_{filter:s}_c{ccd:>02s}_r2404p01_skypca-tmpl.fits'
        starflatfile='Y2A1_20150715t0315_{filter:s}_c{ccd:>02s}_r2360p01_starflat.fits'
        headfile='f'$CCDNUM_LIST'.head'                                                 
        pcaprefix='binned-fp/Y2T4_20150715t0315_{filter:s}_r2404p01_skypca-binned-fp.fits'
        fi
        if [ "${BAND}" == "u" ]; then
            YEAR=y2                                                                           
            EPOCH=e1                                                                          
            biasfile='D_n20141204t1209_c{ccd:>02s}_r1426p08_biascor.fits'                     
            bpmfile='D_n20141020t1030_c{ccd:>02s}_r1474p01_bpm.fits'                          
            dflatfile='D_n20141020t1030_{filter:s}_c{ccd:>02s}_r1471p01_norm-dflatcor.fits'   
            skytempfile='Y2A1_20140801t1130_{filter:s}_c{ccd:>02s}_r1635p02_skypca-tmpl.fits' 
            starflatfile='Y2A1_20140801t1130_{filter:s}_c{ccd:>02s}_r1637p01_starflat.fits'   
            headfile='f'$CCDNUM_LIST'.head'                                                 
            pcaprefix='binned-fp/Y2A1_20140801t1130_{filter:s}_r1635p02_skypca-binned-fp.fits'
        fi
    fi


    # IMPORTANT: test whether we are on a node where stashCache work properly. 
    # now, we test if /cvmfs/des.ogstorage.ord is available and works properly. If it does,
    # use it for corr_dir and conf_dir
    corr_dir=""
    conf_dir=""
    cat /cvmfs/des.osgstorage.org/pnfs/fnal.gov/usr/des/persistent/stash/test.stashdes.1M > /dev/null 2>&1
    TEST_STASH=$?
    if [ $TEST_STASH -eq 0 ]; then
        corr_dir="/cvmfs/des.osgstorage.org/pnfs/fnal.gov/usr/des/persistent/stash/desdm/calib/"
        conf_dir="/cvmfs/des.osgstorage.org/pnfs/fnal.gov/usr/des/persistent/stash/desdm/config/"
    else
        corr_dir="/pnfs/des/persistent/desdm/calib/"
        #conf_dir="/pnfs/des/persistent/desdm/config/"
	conf_dir="/pnfs/des/persistent/stash/desdm/config/"
    fi
    
# write to confFile
    cat <<EOF >> confFile
[General]
nite: 20141229
expnum: 393047
filter: z
r: 04
p: 11
chiplist: $CCDNUM_LIST
data_dir: /pnfs/des/scratch/${SCHEMA}/dts/
corr_dir: $corr_dir
conf_dir: $conf_dir
exp_dir: /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/
template: D{expnum:>08s}_{filter:s}_{ccd:>02s}_r{r:s}p{p:s}
exp_template: D{expnum:>08s}_{filter:s}_r{r:s}p{p:s}
year: $YEAR
yearb: y2
epoch: $EPOCH
epochb: e1
[crosstalk]
xtalk =  DECam_20130606.xtalk
template =  D{expnum:>08s}_{filter:s}_%02d_r{r:s}p{p:s}_xtalk.fits
replace = DES_header_update.20151120

[pixcorrect]
bias=$biasfile
bpm=$bpmfile
linearity = lin_tbl_v0.4.fits
bf = bfmodel_20150305.fits
flat=$dflatfile

[sextractor]
filter_name = sex.conv
filter_name2 = sex.conv
starnnw_name  = sex.nnw
parameters_name = sex.param_scamp_psfex
parameters_name_psfex = sex.param_psfex
configfile  = sexforscamp.config
parameters_name2 = sex.param.finalcut.20130702
configfile2 = sexgain.config
sexforpsfexconfigfile = sexforpsfex.config

[skyCombineFit]
################3 THIS IS WHAT SHOULD BE CHANGED FOR BINNED FP ################3
#pcafileprefix = pca_mini
pcafileprefix = $pcaprefix

[skysubtract]
pcfilename = $skytempfile
weight = sky

[scamp]
imagflags =  0x0700
flag_mask =   0x00fd
flag_astr =   0x0000
catalog_ref =   GAIA-DR2
default_scamp =  default2.scamp.20140423
head = $headfile

[starflat]
starflat = $starflatfile
[psfex]
#old version with PSFVAR_DEGREES 0
#configfile = configoutcat2.psfex
configfile = configse.psfex

EOF
    
    sed -i -e "/^nite\:/ s/nite\:.*/nite\: ${NITE}/" -e "/^expnum\:/ s/expnum\:.*/expnum\: ${EXPNUM}/" -e "/^filter\:/ s/filter:.*/filter\: ${BAND}/" -e "/^r\:/ s/r:.*/r\: ${RNUM}/" -e "/^p\:/ s/p:.*/p\: ${PNUM}/" confFile
    
    setup fitscombine yanny
# first cut of y2 something else is needed for Y4 images as of Nov 2016
    if [ "${YEAR}" == "y4" ]; then
	setup firstcut Y4N+1
#    setup finalcut Y4A1dev+3
#    setup finalcut Y4A1+3
    else
	setup finalcut Y2A1+4
    fi
    setup ftools v6.17
    setup scamp 2.6.10+0
    setup tcl 8.5.17+0
    setup extralibs 1.0
    setup wcstools 3.8.7.1+2
#to parallelize the ccd loops 
    setup joblib 0.8.4+3
    export MPLCONFIGDIR=$PWD/matplotlib
    mkdir $MPLCONFIGDIR
    mkdir qa
    setup healpy
    setup pandas
    setup astropy 0.4.2+6
    setup scikitlearn 0.14.1+9
    
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${PWD}/usr/lib64/mysql
    
# in case single epoch processing was already done, skip that step
    if [ "$JUMPTOEXPCALIB" == "true" ] ; then
	echo "jumping to the calibration step..."
	nfiles=`ls *_r${RNUM}p${PNUM}_fullcat.fits *_r${RNUM}p${PNUM}_immask.fits.fz | wc -l`
	nccds=`grep chiplist confFile | awk -F ':' '{print $2}' | sed 's/,/ /g' | wc -w`
	nccds2=`expr $nccds \* 2`
	if [  $nfiles -ne $nccds2 ] ; then
	    echo "copying fits files from Dcache"
	    
	    for c in $ccdlist; do
        # copies all ccds
		c=$(printf "%02d" $c)
		filestocopy1="`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_'$c'_r'${RNUM}'p'${PNUM}'_fullcat.fits' | grep fnal | grep fits`"
		echo "filestocopy1: $filestocopy1"
		filestocopy2="`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_'$c'_r'${RNUM}'p'${PNUM}'_immask.fits.fz' | grep fnal | grep fits`"
		echo "filestocopy2: $filestocopy2"
		ifdh cp --force=xrootd -D $filestocopy1 $filestocopy2 .
	    done
	    for file in $(ls *_immask.fits.fz)
	    do
		funpack -D $file
	    done
	fi
    else
	if [ "$SINGLETHREAD" == "true" ] ; then
	    python run_desdmy1e2.py confFile 
	else
	    python run_SEproc.py confFile 
	fi
	RESULT=$?
	if [ $RESULT -ne 0 ] ; then
	    echo "ERROR: Main SE processing has exited abnormally with status $RESULT. The rest of the script will now terminate."
	# cleanup if we are in a grid job (defined as having the GRID_USER environment variable set) to avoid potential timeouts on exit
	    if [ -n "$GRID_USER" ] ; then rm -f *.fits *.fits.fz *.ps *.psf *.xml full_1.cat *.head ; fi
	    exit
	fi
    fi
    
    if [ "$DOCALIB" == "true" ]; then 
	
	setup expCalib
	
	setup python 2.7.9+1
	
	python ./make_red_catlist.py
	echo "make_red_catlist.py finished with exit status $?"
	
# doing this unsetup bit seems to break the BLISS old script. Commenting out for now	
#	for prodlist in healpy astropy  fitsio  matplotlib six python ; do unsetup $prodlist ; done # some attempted version fixing
	
	export CONDA_DIR=/cvmfs/des.opensciencegrid.org/fnal/anaconda2
	source $CONDA_DIR/etc/profile.d/conda.sh
	conda activate des18a
	
#    ./BLISS-expCalib_Y3apass.py --expnum $EXPNUM --reqnum $RNUM --attnum $PNUM --ccd $CCDNUM_LIST
	./BLISS-expCalib_Y3apass-old.py --expnum $EXPNUM --reqnum $RNUM --attnum $PNUM --ccd $CCDNUM_LIST
	
	RESULT=$? 
	echo "BLISS-expCalib_Y3pass-old.py exited with status $RESULT"
	
	files2cp=`ls allZP*r${RNUM}*p${PNUM}*.csv Zero*r${RNUM}*p${PNUM}*.csv D*${EXPNUM}*_ZP.csv D*${EXPNUM}*CCDsvsZPs.png D*${EXPNUM}*NumClipstar.png D*${EXPNUM}*ZP.png`
	if [ "x${files2cp}" = "x" ]; then
            echo "Error, no calibration files to copy!"
	else
            ifdh cp --force=xrootd -D $files2cp /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM} || echo "ifdh cp of calibration csv and png files failed. There could be problems with Diffimg down the road when using this exposure."
	fi
    fi
    
    du -sh .
    
# cleanup if we are in a grid job (defined as having the GRID_USER environment variable set) to avoid potential timeouts on exit
    if [ -n "$GRID_USER" ] ; then rm -f *.fits *.fits.fz *.ps *.psf *.xml full_1.cat *.head ; fi
#rm *.csv *.png
    
    export HOME=$OLDHOME
fi # end the if statement that skips SE processing    

# exit now if SE processing a template
if [ "$TEMPLATE" == "true" ]; then
    echo "Finished SE processing template image; exiting before verifySE steps"
    exit 0
fi


######## CODE FORMERLY IN verifySE.sh ##########
echo "***** BEGIN VERIFYSE *****"

export RNUM=$RNUM
export PNUM=$PNUM
export EXPNUM=$EXPNUM
export NITE=$NITE


# copy over the copy_pairs script so we know the templates
ifdh cp ${IFDHCP_OPT} -D /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files/copy_pairs_for_${EXPNUM}.sh  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${EXPNUM}.out ./ || { echo "failed to copy WS_diff.list and copy_paris_for_$EXPNUM}.sh files" ; exit 2 ; }  # do we want to exit here?

TEMPLATEPATHS=`cat copy_pairs_for_${EXPNUM}.sh | sed -r -e "s/ifdh\ cp\ (\-\-force=xrootd\ )?\-D\ //" -e "s/[0-9]{6}\.out//g" | sed -e 's/\$TOPDIR_WSTEMPLATES\/pairs\///'`

for templatedir in $TEMPLATEPATHS
do
    
    tempexp=$(echo $templatedir | egrep -o "\/[0-9]{6}\/$" | tr -d "/")
    echo "Checking exposure $tempexp"
    
# note that $templatedir has a trailing slash already
    
# check for all of the necessary files
    
# first check immask because if they aren't there, we know it's a failure and there's no point doing the rest of it
    immaskfiles="$(ifdh ls ${templatedir}'*_r'${RNUM}'p'${PNUM}'_immask.fits.fz' | grep fits | grep fnal)"
    nimmask=`echo $immaskfiles | wc -w`
    if [ $nimmask -lt 59 ]; then
        ### OK, we're missing the .fz files. Maybe there are uncompressed (.fits) files. Let's check for those too.
        immaskfiles="$(ifdh ls ${templatedir}'*_r'${RNUM}'p'${PNUM}'_immask.fits' | grep fits | grep fnal)"
        nimmask=`echo $immaskfiles | wc -w`
        if [ $nimmask -lt 59 ]; then
            echo "Exposure $tempexp missing one or more immask.fits files. Editing copy_pairs_for_${EXPNUM}.sh and WS_diff.list to remove this exposure. Diffimg will not consider it as a template."
            sed -i -e "s:${templatedir}${tempexp}.out::" copy_pairs_for_${EXPNUM}.sh
            FAILEDEXPS="$FAILEDEXPS $tempexp"
            continue
        else
            echo ".fits files are present. It is a good idea to run fpack on these files and save them in their compressed state in dCache to save space."
        fi
    fi
    
#ok, now check the psf and csv files, but only if we need them
    if [ "$DOCALIB" == "true" ]; then
	
	psffiles="$(ifdh ls ${templatedir}'*_r'${RNUM}p${PNUM}_fullcat.fits | grep fullcat | grep fnal)"
	npsf=`echo $psffiles | wc -w`
        if [ $npsf -lt 59 ]; then
            echo "Exposure $tempexp missing one or more fullcat.fits files. Editing copy_pairs_for_${EXPNUM}.sh and WS_diff.list to remove this exposure. Diffimg will not consider it as a template."
            sed -i -e "s:${templatedir}${tempexp}.out::" copy_pairs_for_${EXPNUM}.sh
            FAILEDEXPS="$FAILEDEXPS $tempexp"
            continue
        fi	
    fi
    
    if [ "${STARCAT_NAME}" != "" ] || [ "${SNVETO_NAME}" != "" ]; then
	csvfiles="$(ifdh ls ${templatedir}'allZP_D*'${tempexp}'*_r'${RNUM}p${PNUM}'*.csv' | grep csv | grep fnal) $(ifdh ls ${templatedir}'Zero_*'${tempexp}'*_r'${RNUM}p${PNUM}'*.csv' | grep csv | grep fnal) $(ifdh ls ${templatedir}'D*'${tempexp}'*_r'${RNUM}p${PNUM}'*_ZP.csv' | grep csv | grep fnal)"
	
	ncsv=`echo $csvfiles | wc -w`
	if [ $ncsv -lt 3 ]; then
	    echo "Exposure $tempexp missing one or more required csv files. Editing copy_pairs_for_${EXPNUM}.sh and WS_diff.list to remove this exposure. Diffimg will not consider it as a template."
	    sed -i -e "s:${templatedir}${tempexp}.out::" copy_pairs_for_${EXPNUM}.sh
	    FAILEDEXPS="$FAILEDEXPS $tempexp"
	    continue
	else
	    ifdh cp ${IFDHCP_OPT} -D $csvfiles ./ || echo "WARNING: Copy of csv files for exposure ${tempexp} failed with status $?"
	fi
#	# We also need to see if these are per-CCD csv files that we need to combine for makestarcat.py. Namely, the D*_ZP.csv files
	if [ ! -s D${tempexp}_r${RNUM}p${PNUM}_ZP.csv ]; then
	    echo "Combining D${tempexp}_CCD_r${RNUM}p${PNUM}_ZP.csv files..."
	    echo "ID,EXPNUM,CCDNUM,NUMBER,ALPHAWIN_J2000,DELTAWIN_J2000,FLUX_AUTO,FLUXERR_AUTO,FLUX_PSF,FLUXERR_PSF,MAG_AUTO,MAGERR_AUTO,MAG_PSF,MAGERR_PSF,SPREAD_MODEL,SPREADERR_MODEL,FWHM_WORLD,FWHMPSF_IMAGE,FWHMPSF_WORLD,CLASS_STAR,FLAGS,IMAFLAGS_ISO,ZeroPoint,ZeroPoint_rms,ZeroPoint_FLAGS" > D${tempexp}_r${RNUM}p${PNUM}_ZP.csv
	    for csvfile in $(ls  D${tempexp}_[0-6][0-9]_r${RNUM}p${PNUM}_ZP.csv)
	    do 
		awk '(NR>1) { print $0 }' $csvfile >>  D${tempexp}_r${RNUM}p${PNUM}_ZP.csv
	    done
	fi
    fi
done
    
    
# if any of them failed remove them from WS_diff.list
for failedexp in $FAILEDEXPS
do
    sed -i -e "s/${failedexp}//"  ./WS_diff.list
    OLDCOUNT=`awk '{print $1}'  WS_diff.list`
    NEWCOUNT=$((${OLDCOUNT}-1))
    sed -i -e s/${OLDCOUNT}/${NEWCOUNT}/ WS_diff.list
done

setup esutil
setup numpy 1.9.1+8
setup gw_utils 1.1
setup extralibs

# run make starcat and difference imaging ccd-by-ccd (in case of comma-separated ccd list)
# in most cases, this list will only be 1 ccd long (and the 1-ccd runs will be run in parallel)
for c in $ccdlist; do
    c=$(printf "%02d" $c)
    
    export HOME=$OLDHOME
    
    ######## CODE FORMERLY IN RUN_DIFFIMG_PIPELINE.sh ##########
    
    ### now we want to make the local directory structure by copying in 
    
    #copy some of the top dir list files and such
    
    # .ini sets up database access (with passwords)
    # WS_diff.list created by dagmaker
    # run_inputs.tar.gz for each exposure number, copies over all the scripts in order to run
    #### revised tar file 20180203
    filestocopy="/pnfs/des/resilient/${SCHEMA}/db-tools/desservices.ini /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${EXPNUM}_run_inputs.tar.gz"
    ifdh cp ${IFDHCP_OPT} -D $filestocopy ./ || { echo "ifdh cp failed for SN_mon* and such" ; exit 1 ; }
    tar zxf ${EXPNUM}_run_inputs.tar.gz
    
    # set environment location
    LOCDIR="${procnum}/${BAND}_`printf %02d $CCDNUM_LIST`"

    # for backwards compatibility tar file copy stuff; not needed anymore
    if [ ! -d ${procnum}/input_files ]; then
        echo "${procnum}/input_files does not exist. This is probably an older input tar file. Create and copy from dCache."
        
        inputfiles=$(ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files/ )
        for ifile in $inputfiles 
        do
            basefile=`basename $ifile`
            if [ "${basefile}" == "input_files" ] || [ -z "$basefile" ]; then
		echo "skipping dir itself"
            else
		ifdh cp ${IFDHCP_OPT} -D /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files/${basefile} ./${procnum}/input_files/ || exit 2
            fi
        done  
    fi
    
    # make symlinks to these files
    ln -s ${procnum}/input_files/* .
    
    # make some local directories expected by the diffimg pipeline
    mkdir ${LOCDIR}/headers ${LOCDIR}/ingest ${LOCDIR}/$(basename $(ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${LOCDIR}/stamps* 0 | head -1))
    ln -s ${LOCDIR}/ingest ${LOCDIR}/stamps* ${LOCDIR}/headers .
    
    /cvmfs/grid.cern.ch/util/cvmfs-uptodate /cvmfs/des.opensciencegrid.org # make sure we have new version of cvmfs
    
    # setup scripts
    source /cvmfs/des.opensciencegrid.org/2015_Q2/eeups/SL6/eups/desdm_eups_setup.sh
    export EUPS_PATH=/cvmfs/des.opensciencegrid.org/eeups/fnaleups:$EUPS_PATH
    
    # setup a specific version of perl so that we know what we're getting
    setup perl 5.18.1+6 || exit 134

    ###### any additional required setups go here #####

    #we will want the GW version of diffimg for sure
    setup perl 5.18.1+6
    setup Y2Nstack 1.0.6+18
    setup diffimg $DIFFIMG_VERSION #whatever the version number ends up being
    setup ftools v6.17  # this is the heasoft stuff
    export HEADAS=$FTOOLS_DIR
    setup autoscan v3.2+0
    setup easyaccess
    setup extralibs 1.0
    setup numpy 1.9.1+8
    #setup gw_utils
    setup scamp 2.6.10+0

    export EUPS_PATH=/cvmfs/des.opensciencegrid.org/eeups/fnaleups:$EUPS_PATH
    export SCAMP_CATALOG_DIR=/cvmfs/des.osgstorage.org/stash/fnal/SNscampCatalog

    # have to set the PFILES variable to be a local dir and not something in CVMFS
    mkdir syspfiles
    ln -s ${FTOOLS_DIR}/syspfiles/* ./syspfiles
    export PFILES=${PWD}/syspfiles

    echo "EUPS setup complete"

    # setup lots more environment variables
    export DES_SERVICES=${PWD}/desservices.ini
    export DES_DB_SECTION=db-sn-test
    export DIFFIMG_HOST=FNAL
    # use catalog dir in stashCache if StashCache works on this worker node. Otherwise fall back to regular CVMFS.
    # try to read testfile in stashcache ; to check if StashCache is correct version and readable
    STASHTEST=$( cat /cvmfs/des.osgstorage.org/stash/test.stashdes.1M > /dev/null 2>&1)
    if [ $? -eq 0 ] && [ -d /cvmfs/des.osgstorage.org/stash/fnal/SNscampCatalog ]; then
        export SCAMP_CATALOG_DIR=/cvmfs/des.osgstorage.org/stash/fnal/SNscampCatalog  
    else
        export SCAMP_CATALOG_DIR=/cvmfs/des.opensciencegrid.org/fnal/SNscampCatalog
    fi
    export AUTOSCAN_PYTHON=$PYTHON_DIR/bin/python
    export DES_ROOT=${PWD}/SNDATA_ROOT/INTERNAL/DES
    export TOPDIR_SNFORCEPHOTO_IMAGES=${PWD}/data/DESSN_PIPELINE/SNFORCE/IMAGES
    export TOPDIR_SNFORCEPHOTO_OUTPUT=${PWD}/data/DESSN_PIPELINE/SNFORCE/OUTPUT
    export TOPDIR_DATAFILES_PUBLIC=${PWD}/data/DESSN_PIPELINE/SNFORCE/DATAFILES_TEST
    export TOPDIR_WSTEMPLATES=${PWD}/WSTemplates
    export TOPDIR_TEMPLATES=${PWD}/WSTemplates
    export TOPDIR_SNTEMPLATES=${PWD}/SNTemplates
    export TOPDIR_WSRUNS=${PWD}/data/WSruns
    export TOPDIR_SNRUNS=${PWD}/data/SNruns

    # these vars are for the make pair function that we pulled out of makeWSTemplates.sh
    TOPDIR_WSDIFF=${TOPDIR_WSTEMPLATES}
    echo "TOPDIR_WSDIFF $TOPDIR_WSDIFF"
    DATADIR=${TOPDIR_WSDIFF}/data             # DECam_XXXXXX directories
    CORNERDIR=${TOPDIR_WSDIFF}/pairs          # output XXXXXX.out and XXXXXX-YYYYYY.out
    ETCDIR=${DIFFIMG_DIR}/etc                 # parameter files
    CALDIR=${TOPDIR_WSDIFF}/relativeZP        # relative zeropoints
    MAKETEMPLDIR=${TOPDIR_WSDIFF}/makeTempl   # templates are made in here

    XY2SKY=${WCSTOOLS_DIR}/bin/xy2sky
    AWK=/bin/awk

    # snana stuff for fakes KRH: do we need this? comment out for now on sep 8
    # export SNANA_DIR=/data/des20.b/data/kessler/snana/snana
    # export SNDATA_ROOT=/data/des20.b/data/SNDATA_ROOT
    # export PATH=${PATH}:${SNANA_DIR}/bin 
    # export PATH=${PATH}:${SNANA_DIR}/util 

    mkdir -p ${TOPDIR_SNFORCEPHOTO_IMAGES}
    mkdir -p WSTemplates/data
    mkdir SNTemplates
    mkdir -p data/WSruns
    mkdir -p data/SNruns
    mkdir -p SNDATA_ROOT/INTERNAL/DES
    mkdir -p ${TOPDIR_WSDIFF}/makeTempl
    mkdir -p ${TOPDIR_WSDIFF}/pairs

    mkdir -p ${TOPDIR_SNFORCEPHOTO_IMAGES}/${NITE} $DES_ROOT $TOPDIR_SNFORCEPHOTO_OUTPUT $TOPDIR_DATAFILES_PUBLIC

    # now copy in the template files
    # so now what we are going to do is copy in the .out files from the overlap_CCD part, and then use those to build the pairs, only 

    # copy in all possible template combinations for exposure, overlap
    # overlap calculation is not done on per-ccd basis because it takes too long - instead it's faster to do all of them within this job
    # copy files and symlink them
    for overlapfile in $(cat ./${procnum}/input_files/copy_pairs_for_${EXPNUM}.sh)
    do
        if [[ $overlapfile =~ [0-9]{6}.out$ ]] ; then
        filebase=$(basename $overlapfile)
        if [ -e ${PWD}/overlap_outfiles/$filebase ]; then ln -s ${PWD}/overlap_outfiles/$filebase  $TOPDIR_WSTEMPLATES/pairs/ ; fi
        fi
    done

    if [ ! "$(ls -A  $TOPDIR_WSTEMPLATES/pairs)" ]; then
        echo "executing copy_pairs.sh at `date`"
        source ./${procnum}/input_files/copy_pairs_for_${EXPNUM}.sh || { echo "Error in copy_pairs_for_${EXPNUM}.sh. Exiting..." ; exit 2 ; }
    fi

    #show output of copy
    echo "contents of pairs directory:"
    ls $TOPDIR_WSTEMPLATES/pairs/

    echo "------"

    ################################
    # create pairs of search and template images
    ################################
    create_pairs() {
    echo "start create_pairs at `date`"
    tstart=`date +%s`
    sexp=$EXPNUM
    dtorad=`echo 45 | ${AWK} '{printf "%12.9f\n",atan2(1,1)/$1}'`
    twopi=`echo 8 | ${AWK} '{printf "%12.9f\n",atan2(1,1)*$1}'`
    echo "now in create_pairs: sexp = $sexp texp = $texp"
        outpair=${CORNERDIR}/${sexp}-${texp}.out
        outpairno=${CORNERDIR}/${sexp}-${texp}.no
      rm -f ${outpair}

          # loop over search CCDs
          #nccd=`wc -l ${CORNERDIR}/${sexp}.out | ${AWK} '{print $1}'`
          #i=1
          #while [[ $i -le $nccd ]]
          #do
          
	    #find ccd corners 
            #sccd=`${AWK} '(NR=='$i'){print $3}' ${CORNERDIR}/${sexp}_${CCDNUM_LIST}.out`
	    sexpfile=""
	    if [ -s ${sexp}_${CCDNUM_LIST}.out ]; then
		sexpfile=${sexp}_${CCDNUM_LIST}.out
	    else
		sexpfile=${CORNERDIR}/${sexp}.out	
	    fi
	    
	    sccd=`${AWK} '($3=='${CCDNUM_LIST}'){print $3}' $sexpfile | head -1`

             # Search CCD RA Dec corner coordinates coverted to radians
            info1=( `${AWK} '($3=='${CCDNUM_LIST}'){printf "%10.7f %10.7f  %10.7f %10.7f  %10.7f %10.7f  %10.7f %10.7f\n",$4*"'"${dtorad}"'",$5*"'"${dtorad}"'",$6*"'"${dtorad}"'",$7*"'"${dtorad}"'",$8*"'"${dtorad}"'",$9*"'"${dtorad}"'",$10*"'"${dtorad}"'",$11*"'"${dtorad}"'"}' $sexpfile | head -1` )
	    
            rm -f tmp.tmp1
            touch tmp.tmp1
     
           j=1
            while [[ $j -le  4 ]]  # loop over 4 corners of the search image chip
            do
       
              thisa=`echo $j | ${AWK} '{print 2*($1-1)}'`
              thisd=`echo $j | ${AWK} '{print 1+2*($1-1)}'`
       
              a1=${info1[$thisa]}
              d1=${info1[$thisd]}
       
              # calculate angular distance (in degrees) of the 4 sides of each CCD
              # ${texp}.out -> ${texp}.sides
              (${AWK} '{printf "%11.8f %11.8f  %11.8f %11.8f  %11.8f %11.8f  %11.8f %11.8f\n",$4*"'"${dtorad}"'",$5*"'"${dtorad}"'",$6*"'"${dtorad}"'",$7*"'"${dtorad}"'",$8*"'"${dtorad}"'",$9*"'"${dtorad}"'",$10*"'"${dtorad}"'",$11*"'"${dtorad}"'"}' ${CORNERDIR}/${texp}.out | ${AWK} '{printf "%10.8f %10.8f %10.8f %10.8f\n",sin($2)*sin($4)+cos($2)*cos($4)*cos($3-$1),sin($2)*sin($6)+cos($2)*cos($6)*cos($5-$1),sin($6)*sin($8)+cos($6)*cos($8)*cos($7-$5),sin($4)*sin($8)+cos($4)*cos($8)*cos($7-$3)}' | ${AWK} '{printf "%11.8f %11.8f %11.8f %11.8f\n",atan2(sqrt(1-$1*$1),$1),atan2(sqrt(1-$2*$2),$2),atan2(sqrt(1-$3*$3),$3),atan2(sqrt(1-$4*$4),$3)}' > ${texp}.sides) >& /dev/null
       
              # calculate angular distance from a1 d1 to each corner of template image
              # ${texp}.out -> ${texp}.dist
             (${AWK} '{printf "%11.8f %11.8f  %11.8f %11.8f  %11.8f %11.8f  %11.8f %11.8f   %2d\n",$4*"'"${dtorad}"'",$5*"'"${dtorad}"'",$6*"'"${dtorad}"'",$7*"'"${dtorad}"'",$8*"'"${dtorad}"'",$9*"'"${dtorad}"'",$10*"'"${dtorad}"'",$11*"'"${dtorad}"'",$3}' ${CORNERDIR}/${texp}.out | ${AWK} '{printf "%10.8f %10.8f %10.8f %10.8f  %2d\n",sin("'"${d1}"'")*sin($2)+cos("'"${d1}"'")*cos($2)*cos("'"${a1}"'"-$1),sin("'"${d1}"'")*sin($4)+cos("'"${d1}"'")*cos($4)*cos("'"${a1}"'"-$3),sin("'"${d1}"'")*sin($6)+cos("'"${d1}"'")*cos($6)*cos("'"${a1}"'"-$5),sin("'"${d1}"'")*sin($8)+cos("'"${d1}"'")*cos($8)*cos("'"${a1}"'"-$7),$9}' | ${AWK} '{printf "%11.8f %11.8f %11.8f %11.8f  %2d\n",atan2(sqrt(1-$1*$1),$1),atan2(sqrt(1-$2*$2),$2),atan2(sqrt(1-$3*$3),$3),atan2(sqrt(1-$4*$4),$4),$5}' > ${texp}.dist) >& /dev/null
        
             # protections for out-of-bounds results to cos/sin when image and template are exactly on top of each other 
             (paste ${texp}.sides ${texp}.dist | ${AWK} -v eps=0.00000001 '{printf "%11.8f %11.8f %11.8f %11.8f  %2d\n",(cos($1)-cos($5)*cos($6))/(sin($5)*sin($6)+eps),(cos($2)-cos($5)*cos($7))/(sin($5)*sin($7)+eps),(cos($3)-cos($7)*cos($8))/(sin($7)*sin($8)+eps),(cos($4)-cos($6)*cos($8))/(sin($6)*sin($8)+eps),$9}' | while read one two three four five ; do eps=0.00000001 ; if [[ "$one" =~ ^[1-9] ]] ; then one=0.99999999 ; elif [[ "$one" =~ ^-[1-9] ]]; then one=-0.99999999 ; fi ; if [[ "$two" =~ ^[1-9] ]] ; then two=0.99999999 ; elif [[ "$two" =~ ^-[1-9] ]] ; then two=-0.99999999 ;  fi ; if [[ "$three" =~ ^[1-9] ]] ; then three=0.99999999 ;  elif [[ "$three" =~ ^-[1-9] ]] ;  then three=-0.99999999 ; fi ; if [[ "$four" =~ ^[1-9] ]] ; then four=0.99999999 ; elif [[ "$four" =~ ^-[1-9] ]] ;  then four=-0.99999999 ;  fi ; echo $one $two $three $four $five  ; done | ${AWK} '{printf "%11.8f %11.8f %11.8f %11.8f  %2d\n",atan2(sqrt(1-$1*$1),$1),atan2(sqrt(1-$2*$2),$2),atan2(sqrt(1-$3*$3),$3),atan2(sqrt(1-$4*$4),$4),$5}' | ${AWK} '($1<10)&&($2<10)&&($3<10)&&($4<10)&&($1+$2+$3+$4>"'"${twopi}"'"*0.95){printf "%6d  %2d  %6d  %2d\n","'"${sexp}"'","'"${sccd}"'","'"${texp}"'",$5}' >> tmp.tmp1) >& /dev/null
        
              j=$[$j+1]

            done # while j [[ ...

            cat tmp.tmp1 | uniq > tmp.tmp2
        mv tmp.tmp2 tmp.tmp1
            n=`wc -l tmp.tmp1 | ${AWK} '{print $1}'`
            if [[ $n -eq 1 ]]
            then
              ${AWK} '(NR==1){printf "%6d  %2d  %6d %2d    %2d\n",$1,$2,$3,'${n}',$4}' tmp.tmp1 >> ${outpair}
            elif [[ $n -gt 1 ]]
            then
              ${AWK} '(NR==1){printf "%6d  %2d  %6d %2d    %2d",$1,$2,$3,'${n}',$4}' tmp.tmp1 >> ${outpair}
              ${AWK} '(NR>1){printf "  %2d",$4}' tmp.tmp1 >> ${outpair}
              echo hi | ${AWK} '{printf "\n"}' >> ${outpair}
            fi
        rm -f ${texp}.{sides,dist} tmp.tmp1
            i=$[$i+1]
          #done #  sccd
      
         # determine if there is an overlap 
         if [[ -f ${outpair} ]]
          then
            echo " ... has overlaps"
            haspairs[$e]=1
          else
            echo " ... has NO overlaps"
            touch ${outpairno}
            haspairs[$e]=0
          fi

      e=$[$e+1]
    echo "create_pairs done at `date`"
    echo "create_pairs took $(( `date +%s` - $tstart )) seconds."
    }  # end create_pairs


    # figure out how many out files we have and make the pairs (exclude the search exposure.out from this list)
    dotoutfiles=$(ls ${TOPDIR_WSDIFF}/pairs/*.out | grep -v "${EXPNUM}-" )
    echo $dotoutfiles

    if [ -z "$dotoutfiles" ]; then
     echo "Error, no .out files to make templates!!!"
    fi

    for dotoutfile in $dotoutfiles
    do
        texp=`basename $dotoutfile | sed -e s/\.out//` # template exposure number
        echo "texp = $texp"
        
    ### link necessary as of diffimg gwdevel7
        mkdir -p ${TOPDIR_WSDIFF}/pairs/$texp
        ln -s $dotoutfile   ${TOPDIR_WSDIFF}/pairs/$texp/
        ln -s $dotoutfile   .
    #make the DECam_$temp_empty directory by default and remove it later if we actually have an overlap for this CCD
    # the "_empty" indicates that there is no overlap
        mkdir  ${TOPDIR_WSDIFF}/data/DECam_${texp}_empty

        # combine the template CCD .out files
        echo check if we already have a combined texp.out file \(in the current directory\)
        texpdotout=$(ls ${texp}.out)
        ntexpdotout=`echo $texpdotout | wc -w`
        if [ $ntexpdotout -lt 1 ]; then
            echo we don\'t have a combined .out, need to generate it by combining the ccd .outs
            ccdfiles=$(ls ${texp}_*.out)
            touch ${texp}.out
            for ccdfile in $ccdfiles
            do
                echo write $ccdfile contents to the .out
                cat $ccdfile >> ${texp}.out
            done
        fi
        echo now that we have a combined .out, we can run create pairs with this template exposure number
        create_pairs
    done

    # now we have the searchexp-overlapexp.out files in the pairs directory so we parse them to see which template/CCD files we actually need in this job

    # link necessary as of diffimg gwdevel7
    ln -s  ${TOPDIR_WSDIFF}/pairs/${EXPNUM}-*.out ${TOPDIR_WSDIFF}/pairs/${EXPNUM}-*.no ${TOPDIR_WSDIFF}/pairs/${EXPNUM}/
    echo "files to loop over for ccd by ccd overlap :"
    ls ${TOPDIR_WSDIFF}/pairs/${EXPNUM}-*.out
    echo "-----"
    # determine overlap ccd by ccd
    for overlapfile in `ls ${TOPDIR_WSDIFF}/pairs/${EXPNUM}-*.out`
    do

        overlapexp=`basename $overlapfile | sed -e s/${EXPNUM}\-// -e s/\.out//`
        overlapnite=$(egrep -o /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/[0-9]{8}/${overlapexp}/${overlapexp}.out ${procnum}/input_files/copy_pairs_for_${EXPNUM}.sh | sed -r -e "s/.*\/([0-9]{8})\/.*/\1/")
        overlapccds=`awk '($2=='${CCDNUM_LIST}') { for( f=5; f<=NF; f++) print $f}' $overlapfile`

        for overlapccd in $overlapccds
        do
        
        
            if [ -d  ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}_empty ]; then
                rmdir  ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}_empty
            fi
            # if overlap, remove "_empty" from filename
            if [ ! -d ${TOPDIR_WSDIFF}/data/DECam_${overlapexp} ]; then
                mkdir  -p ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}
            fi
            file2copy="/pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits.fz"
            ZPdir="/pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapexp}/"
            ZPfilename="D`printf %08d $overlapexp`_`printf %02d $overlapccd`_${rpnum}_ZP.csv"
            ZPfile=${ZPdir}${ZPfilename}
            if [ -z "$ZPfile" ] ; then
                echo "ZP file for the template is not available. We are leaving out this template."
            else
                # if the ZP file for this CCD exists we will symlink it to the generic name, and then copy in fits files
           #     echo "Making symlink to this CCD ZP file..."
           #     currentdir=$(pwd)
           #     cd $ZPdir
           #     ln -s $ZPfilename D`printf %08d $overlapexp`_${rpnum}_ZP.csv
           #     cd $currentdir
		
                if [ -z "$file2copy" ] ; then
                    # backward compatibility
                    echo "WARNING: .fz file for $overlapexp CCD $overlapccd did not appear in ifdh ls and was thus not copied in. Could be a problem. Checking to see if an uncompressed (.fits) file is available."
                    file2copy="/pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits"
                    if [ -z "$file2copy" ] ; then
                        echo  "WARNING: .fits file for $overlapexp CCD $overlapccd did not appear in ifdh ls and was thus not copied in. There could be problems down the road."
                    else
                        ifdh cp ${IFDHCP_OPT} -D $file2copy ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/ || echo "Error in ifdh cp ${IFDHCP_OPT} /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits WSTemplates/data/DECam_${overlapexp}/ !!!"
                        cd  ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/
                        ln -s D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits DECam_`printf %08d ${overlapexp}`_`printf %02d $overlapccd`.fits
                        ln -s D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits DECam_`printf %06d ${overlapexp}`_`printf %02d $overlapccd`.fits
                        fthedit  D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits[0] DOYT delete
                    fi
                # copy the ccd files over
                else
                    ifdh cp ${IFDHCP_OPT} -D $file2copy ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/ || echo "Error in ifdh cp ${IFDHCP_OPT} /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits WSTemplates/data/DECam_${overlapexp}/ !!!"
                    funpack -D ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/`basename $file2copy`
                    cd  ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/
                    # make symlinks to fit naming convention to the expectation of the pipeline
                    ln -s D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits DECam_`printf %08d ${overlapexp}`_`printf %02d $overlapccd`.fits
                    ln -s D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits DECam_`printf %06d ${overlapexp}`_`printf %02d $overlapccd`.fits
                    fthedit  D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits[0] DOYT delete
                fi
                cd ../../../
            fi
        done
    done

    # makestarcat
    if [ "x$STARCAT_NAME" == "x" ]; then
        if [ "x$SNVETO_NAME" == "x" ]; then
        echo "INFO: Neither STARCAT_NAME nor SNVETO_NAME was provided. The makestarcat.py step will NOT run now."
        echo "Please note that these files will not be present if you are expecting them for a diffimg run."
        MAKESTARCAT_RESULT=-1
        else
        echo "WARNING: STARCAT_NAME is set but SNVETO_NAME is not. The SN veto file will be created with the default name."
        python ${GW_UTILS_DIR}/code/makestarcat.py -e $EXPNUM -n $NITE -r $RNUM -p $PNUM -b $BAND --ccd $c -s `echo $procnum | sed -e s/dp//` -snveto $SNVETO_NAME
        #python makestarcat.py -e $EXPNUM -n $NITE -r $RNUM -p $PNUM -b $BAND --ccd $c -s `echo $procnum | sed -e s/dp//` -snveto $SNVETO_NAME
        MAKESTARCAT_RESULT=$?
        fi
    elif [ "x$SNVETO_NAME" == "x" ]; then
        echo "WARNING: STARCAT_NAME is set but SNVETO_NAME is not. The SN veto file will be created with the default name."
        python ${GW_UTILS_DIR}/code/makestarcat.py -e $EXPNUM -n $NITE -r $RNUM -p $PNUM -b $BAND --ccd $c -s `echo $procnum | sed -e s/dp//` -snstar $STARCAT_NAME
        #python makestarcat.py -e $EXPNUM -n $NITE -r $RNUM -p $PNUM -b $BAND --ccd $c -s `echo $procnum | sed -e s/dp//` -snstar $STARCAT_NAME
        MAKESTARCAT_RESULT=$?
    else
        python ${GW_UTILS_DIR}/code/makestarcat.py -e $EXPNUM -n $NITE -r $RNUM -p $PNUM -b $BAND --ccd $c -s `echo $procnum | sed -e s/dp//` -snstar $STARCAT_NAME -snveto $SNVETO_NAME
        #python makestarcat.py -e $EXPNUM -n $NITE -r $RNUM -p $PNUM -b $BAND --ccd $c -s `echo $procnum | sed -e s/dp//` -snstar $STARCAT_NAME -snveto $SNVETO_NAME
        MAKESTARCAT_RESULT=$?
    fi

    # set the STARCAT_NAME and SNVETO_NAME values to the default if one of them wasn't set
    if [ -z "$STARCAT_NAME" ]; then STARCAT_NAME="SNSTAR_${EXPNUM}_${c}_r${RNUM}p${PNUM}.LIST" ; fi
    if [ -z "$SNVETO_NAME"  ]; then SNVETO_NAME="SNVETO_${EXPNUM}_${c}_r${RNUM}p${PNUM}.LIST" ; fi

    if [ $MAKESTARCAT_RESULT -eq 0 ]; then

    # make sure that the files actually exist before we try to copy then. If makestarcat.py did not run, then we won't need to check.
        if [ -f $STARCAT_NAME ] && [ -f $SNVETO_NAME ]; then
        ifdh mkdir /pnfs/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}
        ifdh cp --force=xrootd -D $STARCAT_NAME $SNVETO_NAME /pnfs/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}/ || echo "ERROR: copy of $STARCAT_NAME and $SNVETO_NAME failed with status $?. You may see problems running diffimg jobs later."  
        fi
    else
        if [ $MAKESTARCAT_RESULT -eq -1 ]; then
        echo "makestarcat.py did not run; no SNSTAR or SNVETO files to copy back."
        else
        echo "ERROR: makestarcat.py exited with status $MAKESTARCAT_RESULT. Check the logs for errors. We will NOT copy the output files back."
        fi
    fi
    # end makestarcat

#### begin diffimg proper ####
echo "***** BEGINNING DIFFIMG *****"

    # needed wide survey files
    cp ${GW_UTILS_DIR}/code/WideSurvey_20150908.tar.gz data/WSruns/
    cd data/WSruns
    tar xzf WideSurvey_20150908.tar.gz
    cd -

    ln -s /cvmfs/des.opensciencegrid.org/fnal/relativeZP ${TOPDIR_WSDIFF}/

    # we need a tarball of /data/des30.a/data/WSruns/WideSurvey, which should unwind in data/WSruns
    # copy over run scripts for steps 1-28, give x permissions
    echo "We are in $PWD"
    cp ${LOCDIR}/RUN* ${LOCDIR}/run* ./
    echo "LOCDIR for runs: $LOCDIR"
    chmod a+x RUN[0-9]* RUN_ALL* RUN*COMPRESS*
    # delete leftover logs from previous runs
    rm *.DONE *.LOG

    for runfile in `ls RUN* | grep -v DONE | grep -v LOG | grep -v ".sh"`
    do
        sed -i -e "s@JOBDIR@${PWD}@g" $runfile
    done

    # The WSp1_EXPNUM_FIELD_tileTILE_BAND_CCDNUM_LIST_mh.fits file MUST be in the CWD *and* it MUST be a file, not a symlink!!!!

    # cp the list to WSTemplates 

    # need to get the _diff.list* files in too! They go in WSTemplates/EXPNUM_LISTS/

    mkdir WSTemplates/EXPNUM_LISTS
    mv WS_diff.list WSTemplates/EXPNUM_LISTS/
    # the list file WSTemplates/EXPNUM_LISTS

    # for some reason SEXCAT.LIST is empty when created on des41. Touch it first and then link to it
    touch ${LOCDIR}/INTERNAL_WSTemplates_SEXCAT.LIST
    ln -s ${LOCDIR}/INTERNAL*.LIST .
    ln -s ${LOCDIR}/INTERNAL*.DAT .


    ##### check whether SNSTAR catalog or SNVETO filenames are required. If so, make a symlink if they are in CVMFS. If they are not in CVMFS, copy from dCache directly.
    # if they are given, we are using our own starcat instead of the default one
    SNSTAR_FILENAME=`grep STARSOURCE_FILENAME RUN02_expose_makeStarCat | awk '{print $2}'`
    SNSTAR_FILENAME=`echo $(eval "echo $SNSTAR_FILENAME")`
    OUTFILE_STARCAT=`grep outFile_starCat RUN02_expose_makeStarCat | awk '{print $2}'`
    SNVETO_FILENAME=`grep inFile_veto RUN22_combined+expose_filterObj  | awk '{print $2}'`
    SNVETO_FILENAME=`echo $(eval "echo $SNVETO_FILENAME")`
    # if we are outside the footprint (then SNSTAR_FILENAME and SNVETO_FILENAME are set), we make our own starcat (with gaia), using the BLISS.py outputs
    if [ ! -z "$SNSTAR_FILENAME" ]; then
        cp ${DIFFIMG_DIR}/bin/makeWSTemplates.sh ./
        export PATH=${PWD}:${PATH}
        if [ -s ${SNSTAR_FILENAME} ]; then
            echo "using local copy of SNSTAR"
        else
            head -1 /cvmfs/des.osgstorage.org/pnfs/fnal.gov/usr/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}/${SNSTAR_FILENAME} >/dev/null 2>&1
            HEADRESULT=$?
            if [ $HEADRESULT -eq 0 ]; then
            ln -s /cvmfs/des.osgstorage.org/pnfs/fnal.gov/usr/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}/${SNSTAR_FILENAME} .
            else
            # try to ifdh cp 
            ifdh cp -D ${IFDHCP_OPT} /pnfs/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}/${SNSTAR_FILENAME} ./ || echo "ERROR: ${SNSTAR_FILENAME} is not in CVMFS and there was an error copying it to the worker node. RUN02 will probably fail..."
            fi
        fi
        # image masking for bright galaxy subtraction ; hopefully we don't need this anymore
        sed -i -e "s/0xFFFF/0xFFDF/" -e "s/0x47FB/0x47DB/" SN_makeWeight.param
        sed -i -e '/ZPTEST_ONLY/ a\             -inFile_CALIB_STARS    '"$OUTFILE_STARCAT"' \\' -e "s#\${DIFFIMG_DIR}/etc/SN_makeWeight#${PWD}/SN_makeWeight#" makeWSTemplates.sh
    fi
    if [ ! -z "$SNVETO_FILENAME" ]; then
        if [ -s ${SNVETO_FILENAME} ]; then
            echo "using local copy of SNVETO"
        else
            head -1 /cvmfs/des.osgstorage.org/pnfs/fnal.gov/usr/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}/${SNVETO_FILENAME} >/dev/null 2>&1
            HEADRESULT=$?
            if [ $HEADRESULT -eq 0 ]; then
            ln -s /cvmfs/des.osgstorage.org/pnfs/fnal.gov/usr/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}/${SNVETO_FILENAME} .
            else
            # try to ifdh cp 
            ifdh cp -D ${IFDHCP_OPT} /pnfs/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}/${SNVETO_FILENAME} ./ || echo "ERROR: ${SNVETO_FILENAME} is not in CVMFS and there was an error copying it to the worker node. RUN22 will probably fail..."
            fi
        fi
    fi
    #################
    #copyback function
    #################
    copyback() {

    FPACKFILES=$(ls WS*_template_mh.fits *diff_mh.fits WS*combined*fakeSN*_mh.fits )
    if [ "${FULLCOPY}" == "true" ]; then
        FPACKFILES=$(ls WS*.fits)
    fi
    if [ -z $FPACKFILES ] ; then echo "No expected output files to add!" ; fi

    PACKEDFILES=""
    for file in $FPACKFILES
    do
        fpack -Y $file || echo "Error running fpack on ${file}"
        PACKEDFILES="${file}.fz ${PACKEDFILES}"
    done

    export IFDHCP_OPT="--force=xrootd"

    #set group write permission on the outputs just to be safe
    chmod -R 664 $LOCDIR/*fits*

    #make list of output files
    OUTFILES=""

    #make a tar file of our logs
    TARFILES=""
    if [ $FULLCOPY == "true" ]; then
        TARFILES=$(ls RUN[0-9]* *.cat *out *LIST *xml STARCAT*LIST RUN_ALL.LOG *psf *numList *_ORIG *.lis *.head INTENAL*.DAT ${PACKEDFILES})
    else
        TARFILES=$(ls RUN[0-9]* *.cat *out *LIST *xml STARCAT*LIST RUN_ALL.LOG *psf ${PACKEDFILES})
    fi

    echo "Files to tar: $TARFILES"

    OUTTAR="outputs_${procnum}_${NITE}_${EXPNUM}_${BAND}_$(printf %02d ${CCDNUM_LIST}).tar.gz"
    tar czmf ${OUTTAR} $TARFILES || { echo "Error creating tar file" ; RESULT=1 ; }

    OUTFILES="${OUTTAR} $OUTFILES"

    if [ $RESULT -ne 0 ]; then
        echo "FAILURE: Pipeline exited with status $RESULT "
    fi
    for file in $(ls RUN[0-9]*.FAIL)
    do
        echo "${JOBSUBJOBID} ${JOBSUBPARENTJOBID} $(/bin/hostname)" >> $file
        OUTFILES="${OUTFILES} $file"
    done

    export HOME=$OLDHOME

    echo "outfiles = $OUTFILES"

    if [ ! -z "$OUTFILES" ]; then ifdh cp ${IFDHCP_OPT} -D $OUTFILES /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$LOCDIR/ || echo "FAILURE: Error $? when trying to copy outfiles back" ; fi

    if [ `ls ${TOPDIR_SNFORCEPHOTO_IMAGES}/${NITE} | wc -l` -gt 0 ]; then 
        copies=`ls ${TOPDIR_SNFORCEPHOTO_IMAGES}/${NITE}/ ` 
        ifdh mkdir_p /pnfs/des/${DESTCACHE}/${SCHEMA}/forcephoto/images/${procnum}/${NITE}/${EXPNUM} 
        ifdh cp ${IFDHCP_OPT} -D $copies /pnfs/des/${DESTCACHE}/${SCHEMA}/forcephoto/images/${procnum}/${NITE}/${EXPNUM} || echo "FAILURE: Error $? when copying  ${TOPDIR_SNFORCEPHOTO_IMAGES}"
    fi

    ### also copy back the stamps
    STAMPSDIR=`ls -d $LOCDIR/stamps_*`
    echo "stamps dir: $STAMPSDIR"

    if [ `ls $STAMPSDIR | wc -l` -gt 0 ] ; then
        copies=`ls $STAMPSDIR`
        cd  $STAMPSDIR
        tar czfm `basename ${STAMPSDIR}`.tar.gz *.fits *.gif
        ifdh cp ${IFDHCP_OPT} -D `basename ${STAMPSDIR}`.tar.gz /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$STAMPSDIR || echo "FAILURE: Error $? when copying  ${STAMPSDIR}" 
        cd -
    fi


    IFDH_RESULT=$?
    [[ $IFDH_RESULT -eq 0 ]] || echo "FAILURE: IFDH failed with status $IFDH_RESULT." 

    } # end copyback


    sed -i -e "s/0x47FB/0x47DB/" RUN05_expose_makeWeight
    sed -i -e "/MAXA/ s/1.5/2.0/" SN_cuts.filterObj

    echo "start pipeline"
    #### THIS IS THE PIPELINE!!! #####
    export CCDNUM_LIST
    ./RUN_ALL-${BAND}_`printf %02d ${CCDNUM_LIST}` $ARGS

    #eventually we want
    # perl ${DIFFIMG_DIR}/bin/RUN_DIFFIMG_PIPELINE.pl $ARGS NOPROMPT -writeDB
    # we will leave -writeDB off for testing 1-Jul-2015
    RESULT=$?

    # for failed files
    if [ -e RUN[0-9]*.FAIL ] && [ ! -f RUN28*.FAIL ]; then
    # attempt to clear the database of any failed candidates from this job
        echo $NITE $EXPNUM $BAND $CCDNUM_LIST > failed.list
        if [ -f ${GW_UTILS_DIR}/code/clearfailed_grid_${SCHEMA}.py ]; then
        python ${GW_UTILS_DIR}/code/clearfailed_grid_${SCHEMA}.py -f failed.list -s `echo $procnum | sed -e "s/dp//"` -x
        CLEARFAILED=$?
        echo "Database clearing exited with status $CLEARFAILED"
        fi
        rm failed.list
    fi

    # now check the log files and find the first non-zero RETURN CODE

    for logfile in `ls RUN[0-9]*.LOG`
    do
    CODE=`grep "RETURN CODE" $logfile | grep -v ": 0" | head -1`
    if [ ! -z "${CODE}" ]; then
        echo $logfile $CODE
        exitcode=`echo $CODE | cut -d ":" -f 2`
        touch tmp.fail
        echo "$logfile : $CODE " >> tmp.fail
    ### uncomment this to enable failure on non-zero exit codes
    #    exit $exitcode
    fi
    done
    touch RUN_ALL.FAIL
    if [ -f tmp.fail ] ; then 
        head -1 tmp.fail >> RUN_ALL.FAIL
        rm -f tmp.fail
    else
        echo "NONE" >> RUN_ALL.FAIL
    fi

    copyback



    # let's clean up the work area, especially template directories. Hopefully this will prevent more errors on glexec cleanup. Only do this within a grid job though.

    if [ -n "${GRID_USER}" ]; then
        rm -r WSTemplates
        rm -r $STAMPSDIR
        rm *.fits *.fz *.head *.psf RUN*
    fi

    exit $RESULT
    done

