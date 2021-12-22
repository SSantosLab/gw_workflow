#!/bin/bash

#. /cvmfs/des.opensciencegrid.org/eeups/startupcachejob31i.sh
#setup ftools v6.17
#export HEADAS=$FTOOLS_DIR
#setup wcstools


RNUM="2"
PNUM="01"
SEASON="11"
JOBSUB_OPTS="--memory=3000MB --expected-lifetime=medium --cpu=4 --mail_on_error --email-to=kherner@fnal.gov"
RESOURCES="DEDICATED,OPPORTUNISTIC,OFFSITE"
DIFFIMG_EUPS_VERSION="gwdevel10"
WRITEDB="off"
RM_MYTEMP="false"
IGNORECALIB="false"
DESTCACHE="persistent"
SEARCH_OPTS=""
SCHEMA="gw"
TWINDOW=30.0
TEFF_CUT=0.0
TEFF_CUT_g=0.0
TEFF_CUT_i=0.0
TEFF_CUT_r=0.0
TEFF_CUT_Y=0.0
TEFF_CUT_z=0.0
SKIP_INCOMPLETE_SE="false"
# Added a default to min_nite, on 2017 dec 1st
MIN_NITE=20100101
MAX_NITE=99999999
DO_HEADER_CHECK=1

STASHVER=""

# overwrite defaults if user provides a .rc file
DAGMAKERRC=./dagmaker.rc
if [ -f $DAGMAKERRC ] ; then
    echo "Reading params from config file: $DAGMAKERRC"
    source $DAGMAKERRC
fi

if [ $# -lt 1 ]; then echo "Error, an exposure number must be supplied" ; exit 1; fi
ALLEXPS="$@"
EXPNUM=$1
[[ $EXPNUM =~ ^[0-9]+$ ]] || { echo "Error, exposure number must be a number; you entered $EXPNUM." ; exit 1; }
echo "EXPNUM = $EXPNUM"

if [ ! -d syspfiles_$$ ]; then
    mkdir syspfiles_$$
    ln -s ${FTOOLS_DIR}/syspfiles/* syspfiles_$$
fi
export PFILES=$PWD/syspfiles_$$

NITE=$(awk '($1=='${EXPNUM}') {print $2}' exposures.list)
BAND=$(awk '($1=='${EXPNUM}') {print $6}' exposures.list)

# setup handy  commands
COPYCMD="ifdh cp"
COPYDCMD="ifdh cp -D"
CHMODCMD="ifdh chmod 775"
RMCMD="ifdh rm"
#allow people logged in as desgw to do a straight cp to /pnfs to avoid long lock times
if [ "${USER}" == "desgw" ]; then
    COPYCMD="cp"
    COPYDCMD="cp"
    CHMODCMD="chmod g+w"
    RMCMD="rm -f"
fi

IMGOBJECT=$(gethead /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz OBJECT)
IMGTILING=$(gethead /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz TILING)
imageline=$(awk '($1=='${EXPNUM}') {print $4,$5}' exposures_${BAND}.list)
SEARCHRA=`echo $imageline | cut -d " " -f 1`
SEARCHDEC=`echo $imageline | cut -d " " -f 2 | sed s/+//`
RA10=$(echo "${SEARCHRA}*10" | bc | cut -d "." -f 1)
if [ -z "$RA10" ] ; then RA10=0 ; fi
DEC10=$(echo "$SEARCHDEC * 10" | bc | cut -d "." -f 1)
if [ -z "$DEC10" ] ; then DEC10=0 ; fi
if [ $DEC10 -ge 0 ]; then
    DEC10="+${DEC10}"
fi

NEWFIELD="WS${RA10}${DEC10}"
NEWTILING=1
NEWOBJECT="DESWS hex $NEWFIELD tiling $NEWTILING"

echo "OBJECT = '${NEWOBJECT}'/ Object name" > editfile
echo "FIELD = '${NEWFIELD}'" >> editfile
echo "TILING = 1" >> editfile

   ### first copy the file down
$COPYDCMD /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz ./ && rm -f /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz
for hdr in {1..9} {10..70} 
do
    fthedit "DECam_$(printf %08d ${EXPNUM}).fits.fz[${hdr}]"  @editfile || echo "Error running fthedit for  DECam_`printf %08d ${EXPNUM}`[${hdr}].fits.fz"
done

$COPYCMD DECam_`printf %08d ${EXPNUM}`.fits.fz /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz

if [ $? -eq 0 ]; then
    rm DECam_`printf %08d ${EXPNUM}`.fits.fz
else
    echo "Error copying edited file DECam_`printf %08d ${EXPNUM}`.fits.fz back to dCache!"
    rm DECam_`printf %08d ${EXPNUM}`.fits.fz
    exit 1 
fi

rm -r $PFILES
