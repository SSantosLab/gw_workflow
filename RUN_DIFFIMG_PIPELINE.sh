#!/bin/bash

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

# check for an input argument

echo "Node information: `uname -a`"

if [ $# -lt 1 ]; then
echo "usage: RUN_DIFFIMG_PIPELINE.sh -E EXPNUM -r RPNUM -p season (dpXX) -n NITE -b BAND (i|r|g|Y|z|u) -c ccdlist [-v DIFFIMG_VERSION] [-d destcache (scratch|persistent)] [-m SCHEMA (gw|wsdiff)] [-F]"
exit 1
fi

ARGS="$@"
# we need to pull out expnum, chip, and band : KRH needs to double check syntax here

#rpnum="r1p1"
#procnum="dp91"
DIFFIMG_VERSION="gwdevel13" # can change this with parameter -v <diffimg_version>
DESTCACHE="persistent"
SCHEMA="wsdiff"
FULLCOPY=false
#IFDHCP_OPT="--force=xrootd"
IFDHCP_OPT=""

##### Don't forget to shift the args after you pull these out #####
while getopts "E:c:b:n:r:p:v:d:m:F" opt $ARGS
do case $opt in
    r)
	    rpnum=$OPTARG
	    shift 2
	    ;;
    p)
	    procnum=$OPTARG
	    shift 2
	    ;;
    E)
	    [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: exposure number must be an integer! You put $OPTARG" ; exit 1; }
	    EXPNUM=$OPTARG
	    shift 2
	    ;;
    c)
	    [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: CCD number must be an integer! You put $OPTARG" ; exit 1; }
	    [[ $OPTARG -lt 70 ]] || { echo "Error: the chip number must be less than 70. You entered $OPTARG." ; exit 1; }  
	    CCDNUM_LIST=$OPTARG
	    shift 2

	    ;;
    n)
	    [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: Night must be an integer! You put $OPTARG" ; exit 1; }
	    NITE=$OPTARG
	    shift 2

	    ;;
    v)
	    DIFFIMG_VERSION=$OPTARG
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
    d) # add some checks here
	    DESTCACHE=$OPTARG
	    shift 2
	    ;;
    m)
	    SCHEMA=$OPTARG
	    shift 2
	    ;;
    F)
	    FULLCOPY=true
	    shift
	    ;;
    :)
	    echo "Option -$OPTARG requires an argument."
	    exit 1
	    ;;
esac
done

ARGS="$@"

if [ "x$EXPNUM" == "x" ]; then
echo "Exposure number not set; exiting."
exit 1
fi
if [ "x$CCDNUM_LIST" == "x" ]; then
echo "CCD number not set; exiting."
exit 1
fi
if [ "x$BAND" == "x" ]; then
echo "Band not set; exiting."
exit 1
fi
if [ "x$rpnum" == "x" ]; then
echo "rpnum not set; exiting."
exit 1
fi
if [ "x$procnum" == "x" ]; then
echo "procnum not set; exiting."
exit 1
fi

OLDHOME=$HOME

export HOME=$PWD

### Force use of SLF6 versions for systems with 3.x kernels
case `uname -r` in
    3.*) export UPS_OVERRIDE="-H Linux64bit+2.6-2.12";;
    4.*) export UPS_OVERRIDE="-H Linux64bit+2.6-2.12";;
esac

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
echo "xrdcp version and path now $(xrdcp --version 2>&1) and $(which xrdcp 2>&1)"

#set more environment variables
#for IFDH
export EXPERIMENT=des
export PATH=${PATH}:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/cpn/v1_7/NULL/bin:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/ifdhc/v2_1_0/Linux64bit-2-6-2-12/bin
export PYTHONPATH=/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/ifdhc/v2_1_0/Linux64bit-2-6-2-12/lib/python:${PYTHONPATH}
export IFDH_NO_PROXY=1
export IFDHC_LIB=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v2_1_0/Linux64bit-2-6-2-12/lib
export IFDH_GRIDFTP_EXTRA="-st 1800"

export IFDH_CP_MAXRETRIES=3
export IFDH_XROOTD_EXTRA="-f -N"
export XRD_REDIRECTLIMIT=255
export XRD_REQUESTTIMEOUT=1200
export IFDH_CP_UNLINK_ON_ERROR=1
export IFDHC_CONFIG_DIR=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc_config/v2_1_0/NULL


### now we want to make the local directory structure by copying in 

#copy some of the top dir list files and such

#filestocopy="`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/'SN_mon*.list' | grep fnal.gov` `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/FILTERCHIP.LIST | grep fnal` /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/PROCFILES.LIST  `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_$(printf %02d ${CCDNUM_LIST})/'*.lis' | grep fnal.gov` /pnfs/des/${DESTCACHE}/${SCHEMA}/db-tools/desservices.ini /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${EXPNUM}_run_inputs.tar.gz /pnfs/des/${DESTCACHE}/${SCHEMA}/code/WideSurvey_20150908.tar.gz"

### using resilient pool for files that every job grabs
###filestocopy="`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/'SN_mon*.list' | grep fnal.gov` `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/FILTERCHIP.LIST | grep fnal` /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/PROCFILES.LIST  `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_$(printf %02d ${CCDNUM_LIST})/'*.lis' | grep fnal.gov` /pnfs/des/resilient/${SCHEMA}/db-tools/desservices.ini /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${EXPNUM}_run_inputs.tar.gz"

# .ini sets up database access (with passwords)
# WS_diff.list created by dagmaker
# run_inputs.tar.gz for each exposure number, copies over all the scripts in order to run
#### revised tar file 20180203
filestocopy="/pnfs/des/resilient/${SCHEMA}/db-tools/desservices.ini /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${EXPNUM}_run_inputs.tar.gz"

# /pnfs/des/${DESTCACHE}/${SCHEMA}/code/makeWSTemplates_STARCUT_MAG.sh"

# we do not need /pnfs/des/${DESTCACHE}/${SCHEMA}/code/fakeLib_SNscampCatalog_SNautoScanTrainings_relativeZP.tar.gz as of 20 Sep 2015

#echo filestocopy = $filestocopy
IFDH_DEBUG=1 ifdh cp ${IFDHCP_OPT} -D $filestocopy ./ || { echo "ifdh cp failed for SN_mon* and such" ; exit 1 ; }

#### makeWSTemplates.sh hack
#mkdir makeWSTemplates_STARCUT_MAG
#mv makeWSTemplates_STARCUT_MAG.sh makeWSTemplates_STARCUT_MAG/makeWSTemplates.sh
#chmod a+x  makeWSTemplates_STARCUT_MAG/makeWSTemplates.sh

#mkdir -p ${procnum}/${BAND}_`printf %02d $CCDNUM_LIST`
tar zxf ${EXPNUM}_run_inputs.tar.gz

# set environment location
LOCDIR="${procnum}/${BAND}_`printf %02d $CCDNUM_LIST`"

##mkdir -p ${procnum}/input_files

# move the files from the first ifdh cp into the r1p1 dir to match what is in dCache
# not need with revised tar file 20180203
# mv SN_mon*.list FILTERCHIP* PROCFILES.LIST ${procnum}/

# for backwards compatibility tar file copy stuff; not needed anymore
if [ ! -d ${procnum}/input_files ]; then
    echo "${procnum}/input_files does not exist. This is probably an older input tar file. Create and copy from dCache."
    
    inputfiles=$(ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files/ )
    for ifile in $inputfiles 
    do
	basefile=`basename $ifile`
#echo "basefile = $basefile"
	if [ "${basefile}" == "input_files" ] || [ -z "$basefile" ]; then
	    echo "skipping dir itself"
	else
	    ifdh cp ${IFDHCP_OPT} -D /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files/${basefile} ./${procnum}/input_files/ || exit 2
	fi
    done  
fi

#echo "check input_files:"
# ls -l r1p1/input_files/

# make symlinks to these files
ln -s ${procnum}/input_files/* .

###
#bandfiles=$(ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${LOCDIR}/ )

#for ifile in $bandfiles
#do

#basefile=`basename $ifile`
#echo "basefile = $basefile"
#if [ "${basefile}" == "${BAND}_`printf %02d $CCDNUM_LIST`" ] || [ -z "$basefile" ]; then
#    echo "skipping dir itself"
#else
#    if [ "$basefile" == "headers" ] || [ "$basefile" == "ingest" ] || [[ "$basefile" == "stamps"* ]]; then
#	mkdir ${LOCDIR}/${basefile}
#    else
#	ifdh cp -D /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${LOCDIR}/${basefile}  ./${LOCDIR}/ || exit 2
#    fi
#fi
#done

# make some local directories expected by the diffimg pipeline
mkdir ${LOCDIR}/headers ${LOCDIR}/ingest ${LOCDIR}/$(basename $(ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${LOCDIR}/stamps* 0 | head -1))

#echo "and how about $LOCDIR?"
#ls -l ./${LOCDIR}

ln -s ${LOCDIR}/ingest ${LOCDIR}/stamps* ${LOCDIR}/headers .

#if [ -e ${CONDOR_DIR_INPUT}/templates_for_${EXPNUM}.sh ]; then
#    source ${CONDOR_DIR_INPUT}/templates_for_${EXPNUM}.sh
#else
#    echo "error, the template intput file is missing"
#fi


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
setup gw_utils
setup scamp 2.6.10+0

export EUPS_PATH=/cvmfs/des.opensciencegrid.org/eeups/fnaleups:$EUPS_PATH
export SCAMP_CATALOG_DIR=/cvmfs/des.osgstorage.org/stash/fnal/SNscampCatalog
#export DIFFIMG_DIR=/data/des40.b/data/kherner/Diffimg-devel/diffimg-trunk
#export PATH=`echo $PATH | sed -e s#\/cvmfs\/des.opensciencegrid.org\/eeups\/fnaleups\/Linux64\/diffimg\/gwdevel#\/data\/des40.b\/data/kherner\/Diffimg-devel\/diffimg-trunk#`
#export DIFFIMG_DIR=/data/des41.a/data/marcelle/diffimg/DiffImg-trunk
#export PATH=`echo $PATH | sed -e s#\/cvmfs\/des.opensciencegrid.org\/eeups\/fnaleups\/Linux64\/diffimg\/gwdevel8#\/data\/des41.a\/data/marcelle\/diffimg\/DiffImg-trunk#`

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

#echo "source input copy"


# so now what we are going to do is copy in the .out files from the overlap_CCD part, and then use those to build the pairs, only 

#source ./r1p1/input_files/templates_for_${EXPNUM}.sh

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
      nccd=`wc -l ${CORNERDIR}/${sexp}.out | ${AWK} '{print $1}'`
      i=1
      while [[ $i -le $nccd ]]
      do
        # find ccd corners 
        sccd=`${AWK} '(NR=='$i'){print $3}' ${CORNERDIR}/${sexp}.out`
         # Search CCD RA Dec corner coordinates coverted to radians
        info1=( `${AWK} '($3=='${sccd}'){printf "%10.7f %10.7f  %10.7f %10.7f  %10.7f %10.7f  %10.7f %10.7f\n",$4*"'"${dtorad}"'",$5*"'"${dtorad}"'",$6*"'"${dtorad}"'",$7*"'"${dtorad}"'",$8*"'"${dtorad}"'",$9*"'"${dtorad}"'",$10*"'"${dtorad}"'",$11*"'"${dtorad}"'"}' ${CORNERDIR}/${sexp}.out` )
   
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

   #          echo "template exposure = $texp; search CCD = $sccd; corner = $j"
    
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
      done #  sccd
  
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
    
#make the DECam_$temp_empty directory by default and remove it later if we actually have an overlap for this CCD
# the "_empty" indicates that there is no overlap
    mkdir  ${TOPDIR_WSDIFF}/data/DECam_${texp}_empty
    create_pairs
done

# now we have the searchexp-overlapexp.out files in the pairs directory so we parse them to see which template/CCD files we actually need in this job

ls ${TOPDIR_WSDIFF}/pairs/
# link necessary as of diffimg gwdevel7
ln -s  ${TOPDIR_WSDIFF}/pairs/${EXPNUM}-*.out ${TOPDIR_WSDIFF}/pairs/${EXPNUM}-*.no ${TOPDIR_WSDIFF}/pairs/${EXPNUM}/

# determine overlap ccd by ccd
for overlapfile in `ls ${TOPDIR_WSDIFF}/pairs/${EXPNUM}-*.out`
do
    
    echo "Starting overlap file $overlapfile:"
    cat $overlapfile
    overlapexp=`basename $overlapfile | sed -e s/${EXPNUM}\-// -e s/\.out//`
    overlapnite=$(egrep -o /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/[0-9]{8}/${overlapexp}/${overlapexp}.out ${procnum}/input_files/copy_pairs_for_${EXPNUM}.sh | sed -r -e "s/.*\/([0-9]{8})\/.*/\1/")
    overlapccds=`awk '$2=='${CCDNUM_LIST}'{ for( f=5; f<=NF; f++) print $f}' $overlapfile`
    for overlapccd in $overlapccds
    do
	
	
	if [ -d  ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}_empty ]; then
	    rmdir  ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}_empty
	    fi
    # if overlap, remove "_empty" from filename
	if [ ! -d ${TOPDIR_WSDIFF}/data/DECam_${overlapexp} ]; then
	    mkdir  -p ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}
	fi
#	file2copy=$(ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits.fz | grep fits)
	file2copy="/pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits.fz"

	if [ -z "$file2copy" ] ; then
        # backward compatibility
	    echo "WARNING: .fz file for $overlapexp CCD $overlapccd did not appear in ifdh ls and was thus not copied in. Could be a problem. Checking to see if an uncompressed (.fits) file is available."
#	    file2copy=$(ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits | grep fits)
	    file2copy="/pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits"
	    if [ -z "$file2copy" ] ; then
		echo  "WARNING: .fits file for $overlapexp CCD $overlapccd did not appear in ifdh ls and was thus not copied in. There could be problems down the road."
	    else
		ifdh cp ${IFDHCP_OPT} -D $file2copy ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/ || echo "Error in ifdh cp ${IFDHCP_OPT} /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/*/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits WSTemplates/data/DECam_${overlapexp}/ !!!"
		cd  ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/
		ln -s D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits DECam_`printf %08d ${overlapexp}`_`printf %02d $overlapccd`.fits
		ln -s D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits DECam_`printf %06d ${overlapexp}`_`printf %02d $overlapccd`.fits
		fthedit  D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits[0] DOYT delete
	    fi
    # copy the ccd files over
	else
	    ifdh cp ${IFDHCP_OPT} -D $file2copy ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/ || echo "Error in ifdh cp ${IFDHCP_OPT} /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/*/${overlapexp}/D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits WSTemplates/data/DECam_${overlapexp}/ !!!"
	    funpack -D ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/`basename $file2copy`
	    cd  ${TOPDIR_WSDIFF}/data/DECam_${overlapexp}/
        # make symlinks to fit naming convention to the expectation of the pipeline
	    ln -s D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits DECam_`printf %08d ${overlapexp}`_`printf %02d $overlapccd`.fits
	    ln -s D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits DECam_`printf %06d ${overlapexp}`_`printf %02d $overlapccd`.fits
	    fthedit  D`printf %08d $overlapexp`_${BAND}_`printf %02d $overlapccd`_${rpnum}_immask.fits[0] DOYT delete
	fi
	cd ../../../
    done
done

# needed wide survey files
cp ${GW_UTILS_DIR}/code/WideSurvey_20150908.tar.gz data/WSruns/
cd data/WSruns
tar xzf WideSurvey_20150908.tar.gz
cd -

#untar fakeLib_SNscampCatalog_SNautoScanTrainings.tar.gz 2015-09-20 no longer needed ans it is all in CVMFS now. Just make a link in the relativeZP case
#tar xzf fakeLib_SNscampCatalog_SNautoScanTrainings_relativeZP.tar.gz
#mv relativeZP ${TOPDIR_WSDIFF}/
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
#for file in `ls ./RUN[0-9]* *.cat *.fits *out *LIST *numList *_ORIG ./RUN_ALL.LOG *.lis *.head INTERNAL*.DAT *.psf *.xml`
###for file in `ls ./RUN[0-9]* *.cat *out *LIST *xml STARCAT*LIST ./RUN_ALL.LOG *psf WS*.fits.fz`

#make a tar file of our logs
TARFILES=""
if [ $FULLCOPY == "true" ]; then
    TARFILES=$(ls ./RUN[0-9]* *.cat *out *LIST *xml STARCAT*LIST ./RUN_ALL.LOG *psf *numList *_ORIG *.lis *.head INTENAL*.DAT ${PACKEDFILES})
else
    TARFILES=$(ls ./RUN[0-9]* *.cat *out *LIST *xml STARCAT*LIST ./RUN_ALL.LOG *psf ${PACKEDFILES})
fi

echo "Files to tar: $TARFILES"

OUTTAR="outputs_${procnum}_${NITE}_${EXPNUM}_${BAND}_$(printf %02d ${CCDNUM_LIST}).tar.gz"
tar czmf ${OUTTAR} $TARFILES || { echo "Error creating tar file" ; RESULT=1 ; }

OUTFILES="${OUTTAR} $OUTFILES"

#remove any existing files sin cache otherwise the copyback will fail
#for filetype in  'RUN[0-9]*' '*.cat' '*.fits' '*out' '*LIST' '*numList' '*_ORIG' 'RUN_ALL.LOG' '*.lis' '*.head' 'INTERNAL*.DAT' '*.psf' '*.xml'
#do
#files2rm=`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$LOCDIR/$filetype | awk '{print $1}'`
#if [ ! -z "${files2rm}" ]; then ifdh rm $files2rm || echo "removal of existing files failed." ; fi

#done

if [ $RESULT -ne 0 ]; then
    echo "FAILURE: Pipeline exited with status $RESULT "
fi
for file in $(ls RUN[0-9]*.FAIL)
do
    echo "${JOBSUBJOBID} ${JOBSUBPARENTJOBID} $(/bin/hostname)" >> $file
    OUTFILES="${OUTFILES} $file"
done

# at least try to get the log files back
#    if [ -z "${OUTFILES}" ]; then
#	echo "No outfiles to copy back!"
#    else
#	ifdh cp -D $OUTFILES /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$LOCDIR/ || echo "Error: ifdh copyback of output failed with status $?."
#    fi
#exit $RESULT

export HOME=$OLDHOME

echo "outfiles = $OUTFILES"

if [ ! -z "$OUTFILES" ]; then ifdh cp ${IFDHCP_OPT} -D $OUTFILES /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$LOCDIR/ || echo "FAILURE: Error $? when trying to copy outfiles back" ; fi

#if [ `ls ${TOPDIR_SNFORCEPHOTO_OUTPUT} | wc -l` -gt 0 ]; then
#    # remove existing dir first
#    echo "Contents on TOPDIR_SNFORCEPHOTO_OUTPUT:"
#    ls ${TOPDIR_SNFORCEPHOTO_OUTPUT}
#    filestorm=""
#    for filetorm in `ls ${TOPDIR_SNFORCEPHOTO_OUTPUT}`
#    do 
#	filestorm="/pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$LOCDIR/data/DESSN_PIPELINE/SNFORCE/OUTPUT/${filetorm} $filestorm"
#    done
#    outdircheck=`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$LOCDIR/data/DESSN_PIPELINE/SNFORCE/OUTPUT 0`
#    outdirresult=$?
#    if [ $outdirresult -eq 0 ] ; then 
#	ifdh rm $filestorm || echo "Failed to remove existing /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$LOCDIR/${TOPDIR_SNFORCEPHOTO_OUTPUT} directory."
#    fi
#    # now actually copy
#    ifdh cp -r -D `ls ${TOPDIR_SNFORCEPHOTO_OUTPUT}` /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$LOCDIR/data/DESSN_PIPELINE/SNFORCE/OUTPUT/ || echo "FAILURE: Error $? when copying  ${TOPDIR_SNFORCEPHOTO_OUTPUT}"
#fi
#
if [ `ls ${TOPDIR_SNFORCEPHOTO_IMAGES}/${NITE} | wc -l` -gt 0 ]; then 
    copies=`ls ${TOPDIR_SNFORCEPHOTO_IMAGES}/${NITE}/ ` 
    ifdh mkdir_p /pnfs/des/${DESTCACHE}/${SCHEMA}/forcephoto/images/${procnum}/${NITE}/${EXPNUM} 
    #remove the existing files. We will continue on if we get an error since the file might not exist yet, which is fine. We could get around that by checking if the file exists first, but why spend time doing that if we're going to blow it away anyway

#    for copyfile in $copies ; do ifdh rm  /pnfs/des/${DESTCACHE}/${SCHEMA}/forcephoto/images/${procnum}/${NITE}/$copyfile ; done

    ifdh cp ${IFDHCP_OPT} -D $copies /pnfs/des/${DESTCACHE}/${SCHEMA}/forcephoto/images/${procnum}/${NITE}/${EXPNUM} || echo "FAILURE: Error $? when copying  ${TOPDIR_SNFORCEPHOTO_IMAGES}"
fi

### also copy back the stamps
###dp44/z_25/stamps_20150917_666-643_z_25
STAMPSDIR=`ls -d $LOCDIR/stamps_*`
echo "stamps dir: $STAMPSDIR"

if [ `ls $STAMPSDIR | wc -l` -gt 0 ] ; then
    copies=`ls $STAMPSDIR`
    cd  $STAMPSDIR
    tar czfm `basename ${STAMPSDIR}`.tar.gz *.fits *.gif
#    for copyfile in $copies ; do ifdh rm /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$STAMPSDIR/$copyfile ; done
    ifdh cp ${IFDHCP_OPT} -D `basename ${STAMPSDIR}`.tar.gz /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$NITE/$EXPNUM/$STAMPSDIR || echo "FAILURE: Error $? when copying  ${STAMPSDIR}" 
    cd -
fi


IFDH_RESULT=$?
[[ $IFDH_RESULT -eq 0 ]] || echo "FAILURE: IFDH failed with status $IFDH_RESULT." 

} # end copyback


### makeWSTemplates.sh hack
#export PATH=${PWD}/makeWSTemplates_STARCUT_MAG:${PATH}
#echo "proof this is really here: "
#ls ${PWD}/makeWSTemplates_STARCUT_MAG
#echo "path to makeWSTemplates.sh is `which makeWSTemplates.sh`"

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
