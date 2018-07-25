#!/bin/bash

if [ $# -lt 1 ]; then
    echo "usage: verifySE.sh -E EXPNUM -r RNUM -p PNUM -n NITE -b BAND [-j] [-s] [-S procnum] [-V SNVETO_NAME ] [-T STARCAT_NAME] [-m SCHEMA (gw or wsdiff)] [-d destcache]" 
    exit 1
fi

OLDHOME=$HOME
export HOME=$PWD
DESTCACHE="persistent"
SCHEMA="wsdiff"
#IFDHCP_OPT="--force=xrootd"
IFDHCP_OPT=""
DOCALIB="false"
FAILEDEXPS=""
ulimit -a

##testing a newer version of joblib
##mkdir joblib-0.9.0b4
##ifdh cp -r  /pnfs/des/scratch/marcelle/joblib-0.9.0b4 ./joblib-0.9.0b4
##export PYTHONPATH=$PYTHONPATH:$PWD/joblib-0.9.0b4

# check that xrdcp and uberftb are installed
which xrdcp >/dev/null 2>&1
CHECK_XRDCP=$?
which uberftp >/dev/null 2>&1
CHECK_UBERFTP=$?

# pretend that CHECK_XRDCP failed if we detect version 4.6.0 since it is buggy
XRDCP_VERSION=`xrdcp --version 2>&1`
if [[ $XRDCP_VERSION == *4.6.0* ]] ; then  CHECK_XRDCP=1 ; fi

if [ $CHECK_XRDCP -ne 0 ] || [ $CHECK_UBERFTP -ne 0 ]; then
    if [ -f /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/current/el6-x86_64/setup.sh ]; then
	. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/current/el6-x86_64/setup.sh
    else
	"Cannot find OASIS CVMFS setup file, and xrdcp and/or uberftp are not in the path."
    fi
fi

# 2018-03-09 replace with current OSG stack and 3.4 
. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.4/current/el6-x86_64/setup.sh

source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh
export IFDH_CP_MAXRETRIES=2
export IFDH_XROOTD_EXTRA="-S 4 -f -N"
export XRD_REDIRECTLIMIT=255
export IFDH_CP_UNLINK_ON_ERROR=1

ARGS="$@"
while getopts "E:n:b:r:p:S:d:ChYV:T:m:" opt $ARGS
do case $opt in
    E)
            [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: exposure number must be an integer! You put $OPTARG" ; exit 1; }
            export EXPNUM=$OPTARG
            shift 2
            ;;
    n)
            [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: Night must be an integer! You put $OPTARG" ; exit 1; }
            export NITE=$OPTARG
            shift 2
            ;;
    b)
            case $OPTARG in
                i|r|g|Y|z|u)
                    BAND=$OPTARG
                    shift 2
                    ;;
                *)
                    echo "Error: band option must be one of r,i,g,Y,z,u. You put $OPTARG"
                    exit 1
                    ;;
            esac
            ;;
    r)
            export RNUM=$OPTARG
            shift 2
            ;;
    p)
            export PNUM=$OPTARG
            shift 2
            ;;
    C)
            DOCALIB=true
            shift 
            ;;
    S)
            procnum=$OPTARG
            shift 2
            ;;
    d)
	    DESTCACHE=$OPTARG
	    shift 2
	    ;;
    Y)
	    shift
	    ;;
    h)
	    echo "usage: SE_job.sh -E EXPNUM -r RNUM -p PNUM -n NITE -b BAND -S SEASON [-d scratch|persistent] [-C ] [-V snveto filename] [-T starcat filename]" 
	    exit 1
            ;;
    V)
	    SNVETO_NAME=$OPTARG
	    shift 2
	    ;;
    T)
	    STARCAT_NAME=$OPTARG
	    shift 2
	    ;;
    m)
	    SCHEMA=$OPTARG
	    shift 2
	    ;;
    :)
            echo "Option -$OPTARG requires an argument."
            exit 1
            ;;
esac
done

if [ "x$EXPNUM"  == "x" ]; then echo "Exposure number not set; exiting." ; exit 1 ; fi
if [ "x$NITE"    == "x" ]; then echo "NITE not set; exiting."            ; exit 1 ; fi
if [ "x$BAND"    == "x" ]; then echo "BAND not set; exiting."            ; exit 1 ; fi
if [ "x$RNUM"    == "x" ]; then echo "r number not set; exiting."        ; exit 1 ; fi
if [ "x$PNUM"    == "x" ]; then echo "p number not set; exiting."        ; exit 1 ; fi
if [ "x$procnum" == "x" ]; then echo "season number not set (use -S option); exiting." ; exit 1 ; fi

# copy over the copy_pairs script so we know the templates
ifdh cp ${IFDHCP_OPT} -D /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files/copy_pairs_for_${EXPNUM}.sh  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${EXPNUM}.out ./ || { echo "failed to copy WS_diff.list and copy_paris_for_$EXPNUM}.sh files" ; exit 2 ; }  # do we want to exit here?

TEMPLATEPATHS=`cat copy_pairs_for_${EXPNUM}.sh | sed -r -e "s/ifdh\ cp\ (\-\-force=xrootd\ )?\-D\ //" -e "s/[0-9]{6}\.out//g" | sed -e 's/\$TOPDIR_WSTEMPLATES\/pairs\///'`


for templatedir in $TEMPLATEPATHS
do
    
    tempexp=`echo $templatedir | egrep -o "\/[0-9]{6}\/$" | tr -d "/"`
    
# note that $templatedir has a trailing slash already
    
# check for all of the necessary files
    
# first check immask because if they aren't there, we know it's a failure and there's no point doing the rest of it
    immaskfiles="`ifdh ls ${templatedir}'*_r'${RNUM}'p'${PNUM}'_immask.fits.fz' | grep fits | grep fnal`"
    nimmask=`echo $immaskfiles | wc -w`
    if [ $nimmask -lt 59 ]; then
        ### OK, we're missing the .fz files. Maybe there are uncompressed (.fits) files. Let's check for those too.
        immaskfiles="`ifdh ls ${templatedir}'*_r'${RNUM}'p'${PNUM}'_immask.fits' | grep fits | grep fnal`"
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
    if [ "$DOCALIB" == "true" ] && ( [ "${STARCAT_NAME}" != "" ] || [ "${SNVETO_NAME}" != "" ] ); then
	
        psffiles="`ifdh ls ${templatedir}'*_r'${RNUM}'p'${PNUM}'_fullcat.fits' | grep fullcat | grep fnal`" 
        npsf=`echo $psffiles | wc -w`
        if [ $npsf -lt 59 ]; then
            echo "Exposure $tempexp missing one or more fullcat.fits files. Editing copy_pairs_for_${EXPNUM}.sh and WS_diff.list to remove this exposure. Diffimg will not consider it as a template."
            sed -i -e "s:${templatedir}${tempexp}.out::" copy_pairs_for_${EXPNUM}.sh
            FAILEDEXPS="$FAILEDEXPS $tempexp"
            continue
        fi
        
        csvfiles="`ifdh ls ${templatedir}'allZP_D*'${tempexp}'_r'${RNUM}p${PNUM}'*.csv' | grep csv | grep fnal` `ifdh ls ${templatedir}'Zero_*'${tempexp}'_r'${RNUM}p${PNUM}'*.csv' | grep csv | grep fnal` `ifdh ls ${templatedir}'D*'${tempexp}'_r'${RNUM}p${PNUM}'*_ZP.csv' | grep csv | grep fnal`" 
        ncsv=`echo $csvfiles | wc -w`
        if [ $ncsv -lt 3 ]; then
            echo "Exposure $tempexp missing one or more required csv files. Editing copy_pairs_for_${EXPNUM}.sh and WS_diff.list to remove this exposure. Diffimg will not consider it as a template."
            sed -i -e "s:${templatedir}${tempexp}.out::" copy_pairs_for_${EXPNUM}.sh
            FAILEDEXPS="$FAILEDEXPS $tempexp"
            continue
        else
            ifdh cp ${IFDHCP_OPT} -D $csvfiles ./ || echo "WARNING: Copy of csv files for exposure ${tempexp} failed with status $?"
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

#get rid of the old file so we can insert the new ones
if [ ! -z "${FAILEDEXPS}" ]; then
    echo "Exposures with failed SE processing and/or calibration are $FAILEDEXPS."
    ifdh rm  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list ||  echo "SEVERE WARNING: failed to remove existing WS_diff.list file."
    ifdh rm /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files/copy_pairs_for_${EXPNUM}.sh ||  echo "SEVERE WARNING: failed to remove existing copy_pair file."
    ifdh cp ${IFDHCP_OPT} ./WS_diff.list  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list \; ./copy_pairs_for_${EXPNUM}.sh /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files/copy_pairs_for_${EXPNUM}.sh || echo "SEVERE WARNING: failed to copy back edited WS_diff.list and copy_pairs files. Diffimg may have problems for CCDs depending on templates with the failed exposures\!"
fi

# run the makestarcat step
# NO LONGER NEEDED with the introduction of the gw_utils package.
#ifdh cp -D ${IFDHCP_OPT} /pnfs/des/persistent/${SCHEMA}/code/makestarcat.py ./ || echo "ERROR: error copying makestarcat.py"


setup esutil
setup numpy 1.9.1+8
setup gw_utils
setup extralibs

# run make starcat
if [ "x$STARCAT_NAME" == "x" ]; then
    if [ "x$SNVETO_NAME" == "x" ]; then
	echo "INFO: Neither STARCAT_NAME nor SNVETO_NAME was provided. The makestarcat.py step will NOT run now."
	echo "Please note that these files will not be present if you are expecting them for a diffimg run."
	MAKESTARCAT_RESULT=-1
    else  
	echo "WARNING: STARCAT_NAME is set but SNVETO_NAME is not. The SN veto file will be created with the default name."
	python ${GW_UTILS_DIR}/code/makestarcat.py -e $EXPNUM -n $NITE -r $RNUM -p $PNUM -b $BAND -s `echo $procnum | sed -e s/dp//` -snveto $SNVETO_NAME 
	MAKESTARCAT_RESULT=$?
    fi
elif [ "x$SNVETO_NAME" == "x" ]; then
    echo "WARNING: STARCAT_NAME is set but SNVETO_NAME is not. The SN veto file will be created with the default name."
    python ${GW_UTILS_DIR}/code/makestarcat.py -e $EXPNUM -n $NITE -r $RNUM -p $PNUM -b $BAND -s `echo $procnum | sed -e s/dp//` -snstar $STARCAT_NAME
    MAKESTARCAT_RESULT=$?
else
    python ${GW_UTILS_DIR}/code/makestarcat.py -e $EXPNUM -n $NITE -r $RNUM -p $PNUM -b $BAND -s `echo $procnum | sed -e s/dp//` -snstar $STARCAT_NAME -snveto $SNVETO_NAME
    MAKESTARCAT_RESULT=$?
fi	

# set the STARCAT_NAME and SNVETO_NAME values to the default if one of them wasn't set
if [ -z "$STARCAT_NAME" ]; then STARCAT_NAME="SNSTAR_${EXPNUM}_r${RNUM}p${PNUM}.LIST" ; fi
if [ -z "$SNVETO_NAME"  ]; then SNVETO_NAME="SNVETO_${EXPNUM}_r${RNUM}p${PNUM}.LIST" ; fi

if [ $MAKESTARCAT_RESULT -eq 0 ]; then

# make sure that the files actually exist before we try to copy then. If makestarcat.py did not run, then we won't need to check.
    if [ -f $STARCAT_NAME ] && [ -f $SNVETO_NAME ]; then
	ifdh mkdir /pnfs/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}
	ifdh cp --force=xrootd -D ${IFDHCP_OPT} $STARCAT_NAME $SNVETO_NAME /pnfs/des/persistent/stash/${SCHEMA}/CATALOG_FILES/${NITE}/ || echo "ERROR: copy of $STARCAT_NAME and $SNVETO_NAME failed with status $?. You may see problems running diffimg jobs later."	
    fi
else
    if [ $MAKESTARCAT_RESULT -eq -1 ]; then
	echo "makestarcat.py did not run; no SNSTAR or SNVETO files to copy back."
    else
	echo "ERROR: makestarcat.py exited with status $MAKESTARCAT_RESULT. Check the logs for errors. We will NOT copy the output files back."
    fi
fi

export HOME=$OLDHOME
