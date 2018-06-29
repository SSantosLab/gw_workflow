#!/bin/bash

umask 002

##export LD_PRELOAD=/usr/lib64/libpdcap.so.1

# give an exposure number and generate a DAG to do our full chain. The structure is like so: 

# 1) Given exposure number, calculate which templates we need to run SE procoessing for. Store those in some list.
#
#  2) Check to see which of the templates has already been through SE processing, if any. Remove them from the list.
#
# 3) Set up standard output directory space in dCache and give appropriate permissions to the dirs
#
# 4) Start making the first stage of the DAG
# a) Create a set of parallel jobs that does the SE processing for the new exposure and all of its dependencies that haven't run yet (the list from steps 1-2.)
#    If all templates have already been through SE processing, this section will consist only of the SE processing for the new exposure
# b) at the end add a "dummy" serial jbos that does nothing except set up the proper dependency. It could send a mail or something saying that the SE steps are done
#
# 5) Make the second stage of the DAG
# a) now create 60 parallel jobs (one per chip) and run the full diffimg pipeline within that job
# b) each parallel job runs the same script, but takes the appropriate chip number (templates too?) as arguments
#
# 6) final stage of the DAG: single Runmon job to finalize everything
#
#
#   Visually, DAG is like this:
#
#
#   SEnewexp  SEtemplate1  SEtemplate2 ... SEtemplateN     (could be only SNnewexp if templates are already done)
#     \            |            |              /
#      \           |            |             /
#       \          |            |            /
#        \         |            |           /
#                
#                   dummy job
#                 /     |     \
#                /      |      \
#     Diffimg chip1   .....    Diffimg chip 62              
#                \      |      /
#                 \     |     /
#                RUNEND_monDiffimg  
#
#
#
#
#.

##### helper functions ######


fetch_from_DESDM () {
    OLDPYTHONPATH=$PYTHONPATH
    export PYTHONPATH=/data/des40.b/data/kherner/qatoolkit-trunk/python:${PYTHONPATH}
    
    find_template_images_by_exp.py -s db-dessci -S des_admin --expnum $overlapnum -p Y3A1_FINALCUT --band $BAND --release Y3A2 --use_blacklist --use_eval --outfilename desdm_files_${overlapnum}.list ||  echo "Error $? running python script."
    if [ ! -e  desdm_files_${overlapnum}.list ] ; then continue ; fi
    
    while read url
    do
    FILENAME=`basename $url`
    FILENAME=`echo $FILENAME | sed -r -e "s/r[0-9]+p[0-9]+/r4p4/" -e "s/immasked/immask/" -e "s/_c([0-9]{2})/_\1/"`
    wget -nv --user=kherner --password=krh70chips --ca-directory=/etc/grid-security/certificates $url
    mv `basename $url` $FILENAME
#       check_header
    NITE=`echo $url | cut -d "/" -f 10`
    overlapnum=`echo $url | cut -d "/" -f 11 | sed -e "s/D[0-9][0-9]//"`
    done < desdm_files_${overlapnum}.list
    if [ `wc -l desdm_files_${overlapnum}.list | cut -d " " -f 1` -eq 0 ] ; then 
    rm desdm_files_${overlapnum}.list
    echo "No files available for this exposure from DESDM. Mostly likely it fails a zero point or other quality cut."
    export PYTHONPATH=$OLDPYTHONPATH
    return 1
    fi
    if [ ! -d /pnfs/des/persistent/${SCHEMA}/exp/${NITE}/${overlapnum} ]; then mkdir -p /pnfs/des/persistent/${SCHEMA}/exp/${NITE}/${overlapnum}  ; chmod g+w /pnfs/des/persistent/${SCHEMA}/exp/${NITE}/${overlapnum} ; fi
    ./getcorners.sh $overlapnum ./ ./ && if [ -f /pnfs/des/persistent/${SCHEMA}/exp/${NITE}/${overlapnum}/${overlapnum}.out ]; then rm -f /pnfs/des/persistent/${SCHEMA}/exp/${NITE}/${overlapnum}/${overlapnum}.out ; fi ; cp ${overlapnum}.out /pnfs/des/persistent/${SCHEMA}/exp/${NITE}/${overlapnum}/${overlapnum}.out
    cp D*${overlapnum}*_immask.fits.fz /pnfs/des/persistent/${SCHEMA}/exp/${NITE}/${overlapnum}/ && rm D*${overlapnum}*_immask.fits.fz && chmod g+w /pnfs/des/persistent/${SCHEMA}/exp/${NITE}/${overlapnum}/*.out /pnfs/des/persistent/${SCHEMA}/exp/${NITE}/${overlapnum}/*.fz
    
    rm desdm_files_${overlapnum}.list ${overlapnum}.out
    export PYTHONPATH=$OLDPYTHONPATH
    return 0
}

fetch_noao() {

# first we need the RA and DEC of the image in question
full_imageline=$(egrep "^\s?${overlapnum}" exposures_${BAND}.list)

imageline=$(echo $full_imageline | awk '{print $4,$5}' )
PROPID=$( echo  $full_imageline | awk '{print $8}' )
SEARCHRA=`echo $imageline | cut -d " " -f 1`
SEARCHDEC=`echo $imageline | cut -d " " -f 2`

fetchurl="http://nsaserver.sdm.noao.edu:7001/?instrument=decam&obstype=object&proctype=raw&date=${overlapnite:0:4}-${overlapnite:4:2}-${overlapnite:6:2}&PROPOSAL=${PROPID}&FORMAT=image/fits&RELEASE_STATUS=public"

echo "fetchurl = $fetchurl"
curl -s $fetchurl -o votable_${overlapnum}.xml

sed -i -e s/datatype=\"date\"/datatype=\"char\"/ -e 's/\,/ /g' votable_${overlapnum}.xml

cat <<EOF > get_images_${overlapnum}.py
#!/usr/bin/python
from astropy.io.votable import parse_single_table
from subprocess import Popen
import sys
import os
import math
import subprocess
table=parse_single_table("votable_${overlapnum}.xml")
RA =  $SEARCHRA
DEC = $SEARCHDEC
SEARCHEXP = $overlapnum
j=0
k=0
not_end=1
s_url =[]
s_crval = []
dists = []
while not_end:
    s_url0 = None
    s_crval0 = None
    try:
       s_url0=table.array['access url'][j]
       s_crval0=table.array['CRVAL'][j]
#       print s_url0, s_crval0
    except IndexError:
       not_end=0
#    print s_url0
    if s_url0 != None:    
        s_url1=s_url0.replace("7006","7003")
#        s_url1=s_url0.replace("7506","7003")
        i=s_url1.find("&extension")
        s_url2=s_url1[0:i]
        RADIFF = float(s_crval0[0]) - RA
        # beware the wraparound problem...
        if RADIFF > 180.0 : RADIFF -= 360.0 
        if RADIFF < -180.0 : RADIFF += 360.0 
        DECDIFF = float(s_crval0[1]) - DEC
        if abs(RADIFF) <= 0.1 and abs(DECDIFF) <= 0.1 :
            if s_url2 not in s_url:
                dist=math.hypot(RADIFF,DECDIFF)
                insert_index = 0
                for ii in range(0,len(dists)):
                    if dists[ii] < dist: insert_index +=1
                dists.insert(insert_index, dist)
                s_url.insert(insert_index, s_url2)
                s_crval.insert(insert_index, s_crval0)
 #               print j,  s_url[k], s_crval[k]
                k=k+1
    j=j+1
#
##download files using curl
n_files=0
n_files=k
if n_files < 1:
    print('Error, no images to download!\n')
    sys.exit(1)
print "There are %d image files"  % n_files 
for ifile in range(0,n_files):
     i_fname=s_url[ifile].find("=")
     fname=s_url[ifile][i_fname+1:]
     print "\n **********retreiving image %d " % ifile, s_url[ifile]
     finfile=s_url[ifile]
     print finfile, fname
     expstring=""
     try:
         os.system("curl "+ finfile +" -o " + fname)
     except:
         print("Error downloading file from noao!\n" )
         continue
     #### check if this is really the image that we want
     funhead=subprocess.Popen(["/home/s1/marcelle/bin/funhead",fname],stdout=subprocess.PIPE)
     grepcmd=subprocess.Popen(["grep","EXPNUM"],stdin=funhead.stdout,stdout=subprocess.PIPE)
     funhead.stdout.close()
     expstring=grepcmd.communicate()[0]
     try:
         expstring=expstring.split()[2]
     except:
         print("No EXPNUM in header\n")
         expstring="-1"
     print expstring + "\n"  
     if int(expstring) == SEARCHEXP :
         try:
             os.system("cp " + fname + " /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_00${overlapnum}.fits.fz")
         except:
             print("Error copying file into dCache!\n")
         finally:
             os.system("rm " + fname)
             break # we found the right exposure; no point in looking at the others

     os.system("rm " + fname)   

EOF

python get_images_${overlapnum}.py

return $?

}

check_header() {
    
# we need to see if the "OBJECT" field in the image header in dCache contains the word "hex". If it does not then we need to 
# replace that field in the header with "DES${SCHEMA} hex $FIELD tiling 1"
    
    IMGOBJECT=$(gethead /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz OBJECT)
    IMGTILING=$(gethead /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz TILING)
    
   ### first copy the file down
    $COPYDCMD /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz ./ && rm -f /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz
       imageline=$(egrep "^\s?${EXPNUM}" exposures_${BAND}.list | awk '{print $4,$5}' )
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
}

#### making the dag ####

ARGS="$@"

# read the search exposure number
if [ $# -lt 1 ]; then echo "Error, at least one json file must be specified" ; exit 1; fi

# check args

for arg in $ARGS
do
    if [ ! -f $arg ]; then echo "File $arg not found! Please correct your argument list. Exiting." ; exit 1 ; fi
done

BLISSFILE="copy_BLISS_`date +%Y-%m-%d-T%H%M%S`.list" 
touch $BLISSFILE

# check that all necessary files exist:
requiredfiles=( ~/.pgpass ~/.desservices.ini ~/.wgetrc-desdm )
for requiredfile in ${requiredfiles[*]}
do
    if [ ! -f $requiredfile ] ; then echo "Error: $requiredfile not found." ; exit 2 ; fi
done

# check also the optional files:
optionalfiles=( ./dagmaker.rc )
if [ ! -f $optionalfile ] ; then echo "Warning: $optionalfile not found." ; fi 

# set default parameters
RNUM="2"
PNUM="01"
SEASON="11"
JOBSUB_OPTS="--mail_on_error --email-to=kherner@fnal.gov"
RESOURCES="DEDICATED,OPPORTUNISTIC,OFFSITE,SLOTTEST"
DIFFIMG_EUPS_VERSION="gwdevel13"
JOBSUB_OPTS_SE="--memory=3000MB --expected-lifetime=medium --cpu=4"
WRITEDB="off"
IGNORECALIB="false"
DESTCACHE="persistent"
SE_OPTS=""
SCHEMA="gw"
TWINDOW=30.0
TEFF_CUT=0.0
TEFF_CUT_g=0.0
TEFF_CUT_i=0.0
TEFF_CUT_r=0.0
TEFF_CUT_Y=0.0
TEFF_CUT_z=0.0
# overwrite defaults if user provides a .rc file
DAGMAKERRC=./dagmaker.rc
if [ -f $DAGMAKERRC ] ; then
    echo "Reading params from config file: $DAGMAKERRC"
    source $DAGMAKERRC
fi

# set processing versions
procnum="dp$SEASON"
rpnum="r"$RNUM"p"$PNUM

# print params used in this run
echo "----------------"
echo "SEASON = $SEASON => DIFFIMG proc. version is $procnum"
echo "RNUM = $RNUM , PNUM = $PNUM  => SE proc. version is $rpnum"
echo "WRITEDB = $WRITEDB (default is WRITEDB=off; set WRITEDB=on if you want outputs in db)"  
echo "IGNORECALIB = $IGNORECALIB (default is false)"
echo "JOBSUB_OPTS = $JOBSUB_OPTS"
echo "JOBSUB_OPTS_SE = $JOBSUB_OPTS_SE"
echo "RESOURCES = $RESOURCES"
echo "DIFFIMG_EUPS_VERSION = $DIFFIMG_EUPS_VERSION"
echo "DESTCACHE = $DESTCACHE"
echo "SCHEMA = $SCHEMA"
echo "----------------"

### dummy job
cat <<EOF > dummyjob.sh
echo "I do not actually do anything except say hello."
exit 0
EOF
chmod a+x dummyjob.sh

echo "set up environment, and handy commands"

# pull the setps from setup-diffImg
. /cvmfs/des.opensciencegrid.org/2015_Q2/eeups/SL6/eups/desdm_eups_setup.sh

export PATH=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/kx509/v3_1_0/NULL/bin:/cvmfs/fermilab.opensciencegrid.org/products/common/prd/cigetcert/v1_16_1/Linux64bit-2-6-2-12/bin:/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v1_8_11/Linux64bit-2-6-2-12/bin:/cvmfs/fermilab.opensciencegrid.org/products/common/prd/jobsub_client/v1_2_3_1/NULL:${PATH}
export PYTHONPATH=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v1_8_11/Linux64bit-2-6-2-12/lib/python:/cvmfs/fermilab.opensciencegrid.org/products/common/prd/jobsub_client/v1_2_3_1/NULL:${PYTHONPATH}:/cvmfs/fermilab.opensciencegrid.org/products/common/prd/pycurl/v7_16_4/Linux64bit-2-6-2-12/pycurl

export IFDH_NO_PROXY=1
export CIGETCERTLIBS_DIR=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/cigetcertlibs/v1_1/Linux64bit-2-6-2-12

export EUPS_PATH=/cvmfs/des.opensciencegrid.org/eeups/fnaleups:$EUPS_PATH

# setup a specific version of perl so that we know what we're getting
setup perl 5.18.1+6 || exit 134

# setup other useful packages and env variables
setup Y2Nstack
setup diffimg $DIFFIMG_EUPS_VERSION
setup ftools v6.17
export HEADAS=$FTOOLS_DIR
setup autoscan
setup astropy
export DIFFIMG_HOST=FNAL
#for IFDH
export EXPERIMENT=des
export PATH=${PATH}:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/cpn/v1_7/NULL/bin:/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v1_8_11/Linux64bit-2-6-2-12/bin
export PYTHONPATH=${PYTHONPATH}:/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v1_8_11/Linux64bit-2-6-2-12/lib/python
export IFDH_NO_PROXY=1
setup wcstools
export PYTHONPATH=${PYTHONPATH}:/data/des40.b/data/kherner/qatoolkit-trunk/python
export PATH=${PATH}:/data/des40.b/data/kherner/qatoolkit-trunk/bin 
setup numpy 1.9.1+8
setup despydb 2.0.0+4

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

echo "prep the list files"

# create the exposures.list file, if it doesn't already exist
if [ ! -f exposures.list ]; then
    ./getExposureInfo.sh
    # and remove the diff.list2 to make sure it stays in sync with the new .list file
    rm -f ./KH_diff_RADEC.list2
fi

echo "figure out overlaps"

#### now run the single exposure script to get the overlaps
###if [ ! -d mytemp_${EXPNUM} ] ; then
###    mkdir mytemp_${EXPNUM}
###fi
###cd mytemp_${EXPNUM}
###ln -s ../exposures_${BAND}.list .

egrep -i "\"RA\"\s*\:" $ARGS | sed -r "s/.*\"RA\"\s*\:(.*)\,/\1/g" > ra.list
egrep -i "\"dec\"\s*\:" $ARGS | sed -r "s/.*\"dec\"\s*\:(.*)\,/\1/g" > dec.list
egrep -i "\"filter\"\s*\:" $ARGS | sed -r "s/.*\"filter\"\s*\:\s*\"([a-zY])\"\,/\1/g" > band.list

paste ra.list dec.list band.list | sort | uniq > ra_dec_band_sorted.list

 ./getOverlaps_RA_DEC.csh ra_dec_band_sorted.list

#### create the output dag file (empty)
outfile=SE_jobs_${rpnum}_${SEASON}.dag
if [ -f $outfile ]; then
    rm $outfile   # maybe we don't want to overwrite? think about that a bit
fi
###touch $outfile
###
#### create the output copy_pairs file (empty)
###templatecopyfile="copy_pairs_for_${EXPNUM}.sh"
###if [ -f $templatecopyfile ]; then
###    rm $templatecopyfile   # maybe we don't want to overwrite? think about that a bit
###fi
###touch $templatecopyfile
###
#### begin composing the dag 
echo "<parallel>" >> $outfile
# stick a dummy job in here so that there is something just in case there ends up being nothing to do for parallel processing
echo "jobsub -n --group=des --OS=SL6  --resource-provides=usage_model=${RESOURCES} --memory=500MB --disk=100MB --expected-lifetime=600s $JOBSUB_OPTS file://dummyjob.sh" >> $outfile
###
#### initialize empty list of files for the copy pairs output
###DOTOUTFILES=""
###
###echo "loop over the diff list of exposures"
###
# now loop over the diff list, get info about the overlaping exposures, and set the SE portion of the dag
for((i=1; i<=`wc -l KH_diff_RADEC.list2 | awk '{print $1}'`; i++)) 
do

    # get expnum, nite info
    overlapnum=$(awk "NR == $i {print \$1}" KH_diff_RADEC.list2)
    overlapnite=$(awk "NR == $i {print \$2}" KH_diff_RADEC.list2)
    BAND=$(awk "NR == $i {print \$3}" KH_diff_RADEC.list2)
    # try to use this exposure 
    SKIP=false
    if [ -z "${overlapnum}" ] && [ -z "${overlapnite}" ] ; then continue ;  fi

    # check that exposure is 30 seconds or longer
    explength=$(egrep "^\s?${overlapnum}" exposures.list | awk '{print $7}')
    explength=$(echo $explength | sed -e 's/\.[0-9]*//' )
    if [ $explength -lt 30 ]; then SKIP=true ; fi
# set the TEFF cut based on the band
    case $BAND in
    g)
        TEFF_CUT=$TEFF_CUT_g
        ;;
    i)
        TEFF_CUT=$TEFF_CUT_i
        ;;
    r)
        TEFF_CUT=$TEFF_CUT_r
        ;;
    Y)
        TEFF_CUT=$TEFF_CUT_Y
        ;;    
    z)
        TEFF_CUT=$TEFF_CUT_z
        ;;
    esac
    echo "Setting t_eff cut to $TEFF_CUT"
    # check that exposure's t_eff is greater than 0.25
   
   
    teff=$(egrep "^\s?${overlapnum}" exposures.list | awk '{print $10}')
    if [ "${teff}" == "NaN" ]; then
    SKIP=true
    echo "Invalid value for t_eff. We will not use this image."
    elif [ $(echo "$teff < $TEFF_CUT" | bc ) -eq 1 ]; then 
    SKIP=true
    echo "This image has a t_eff of $teff, below the cut value of $TEFF_CUT. We will not use this image."
    fi

###    # the first image in the list is the search image itself
###    if [ $i == 1 ]; then 
### if [ "$SKIP" == "true" ] ; then echo "Cannot proceed without the search image!" ; exit 1 ; fi
### NITE=$overlapnite  # capitalized NITE is the search image nite
###    fi
    
    # image failed quality tests ; try the next exposure in the list
    if [ "$SKIP" == "true" ] ; then echo "Overlap exposure $overlapnum failed quality criteria. Skipping." ; continue ; fi

    #### at this point, the image passed basic quality cuts. let's now check if it was not already SE processed:

#    echo -e "\noverlapnum = ${overlapnum} , overlapnite = ${overlapnite} , explength = $explength, teff = $teff"

    # ls in the dcache scratch area to see if images are already there
    nfiles=0    
    for file in `ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/*_${rpnum}_immask.fits.fz`
    do
    if [ `stat -c %s $file` -gt 0 ]; then nfiles=`expr $nfiles + 1` ; touch $file ; fi  
    done

    # ls in the dcache scratch area to see if sextractor files are already there
    mfiles=0    
    for file in `ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/*_${rpnum}_fullcat.fits*`
    do
    if [ `stat -c %s $file` -gt 0 ]; then mfiles=`expr $mfiles + 1` ; touch $file ; fi  
    done

    # check the .out file too
    if [ -e /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/${overlapnum}.out ]; then
    touch /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/${overlapnum}.out 
    else
    # if all the fits files are there, try to produce the missing .out file quickly
    if [ $nfiles -ge 59 ] ; then
        ./getcorners.sh $overlapnum $rpnum /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}
        if [ $? -ne 0 ] ; then 
        echo "Warning: Missing .out file: /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/${overlapnum}.out" 
        # assume something went wrong with the previous SE proc for this image. set nfiles=0 to force reprocessing
        nfiles=0
        fi
    fi
    fi
    
    if [[ $SE_OPTS == *-C* ]]; then
    # check if calibration outputs are present
    # if number of reduced images and sextractor catalogs is not the same, something looks fishy. set nfiles=0 to force reprocessing 
    if [ $mfiles -ne $nfiles ] ; then nfiles=0 ; fi
    JUMPTOEXPCALIBOPTION=""
    if [ -e /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/allZP_D`printf %08d ${overlapnum}`_${rpnum}.csv ]; then
        touch /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/allZP_D`printf %08d ${overlapnum}`_${rpnum}.csv 
    else
        # if only the expCalib outputs are missing and we are not allowed to ignore them
            if [ $nfiles -ge 59 ] && [ "$IGNORECALIB" == "true" ] ; then
            # assume something went wrong with the previous SE proc for this image (set nfiles=0 to force reprocessing)
        nfiles=0
        # but assume that only calibration step needs to be done for this exposure
        JUMPTOEXPCALIBOPTION="-j"
        echo "Warning: Missing outputs of expCalib. Will jump directly to the calibration step for this image."
            fi
    fi
    fi
    # if there are 59+ files with non-zero size, a .out file, and expCalib outputs, then don't do the SE job again for that exposure     
    if [ $nfiles -ge 59 ]; then
    echo "SE proc. already complete for exposure $overlapnum"
    continue
    fi

### try fetching inputs from DESDM. If the fetch function returns 0, call it good.
    fetch_from_DESDM
    if [ $? -eq 0 ] ; then
    echo "DESDM fetching for $overlapnum was successful."
    ALLGOOD=true
    continue
    fi
#### see if the BLISS processing exists for this exposure, copy and use that if so.

if [ $(ls /data/des50.b/data/BLISS/${overlapnum:0:4}00/${overlapnum}/D`printf %08d $overlapnum`_*r1p1_fullcat.fits | wc -l) -ge 59 ] && [ $(ls /data/des50.b/data/BLISS/${overlapnum:0:4}00/${overlapnum}/D`printf %08d $overlapnum`_*r1p1_immask.fits.fz | wc -l) -ge 59 ] ; then
    echo "BLISS outputs found for exposure $overlapnum; we will use those."
    ALLGOOD=true
    
    if [ ! -d /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum} ]; then
    mkdir -p /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}
    chmod 775   /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}
    fi

### This section copies BLISS outputs over, but it is commented out now because we are assuming this is done elsewhere.

##    echo $overlapnum $RNUM $PNUM  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite} >> $BLISSFILE
    
##    for fullcatfile in  $(ls /data/des50.b/data/BLISS/${overlapnum:0:4}00/${overlapnum}/D`printf %08d $overlapnum`_*r1p1_fullcat.fits)
##    do
##  baseout=`basename $fullcatfile | sed -e "s/r1p1/r${RNUM}p${PNUM}/" -e "s/\.fz//"`
##  cp $fullcatfile /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$overlapnite/$overlapnum/$baseout
##    done
##    
##    for csvfile in  $(ls /data/des50.b/data/BLISS/${overlapnum:0:4}00/${overlapnum}/*r1p01*.csv)
##    do
##  baseout=`basename $csvfile | sed -e "s/r1p01/r${RNUM}p${PNUM}/" -e "s/\.fz//"`
##  cp $csvfile /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$overlapnite/$overlapnum/$baseout  
##    done
##    
##    for blissfile in $(ls /data/des50.b/data/BLISS/${overlapnum:0:4}00/${overlapnum}/D`printf %08d $overlapnum`_*r1p1_immask.fits.fz)
##    do
##  baseout=`basename $blissfile | sed -e "s/r1p1/r${RNUM}p${PNUM}/" -e "s/\.fz//"`
##  funpack -O $baseout $blissfile
##  if [ $? -eq 0 ] ; then
##      cp $baseout /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$overlapnite/$overlapnum/$baseout  || ALLGOOD=false
##      rm $baseout
##  else
##      ALLGOOD=false
##  fi
##    done
##    if [ -e /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$overlapnite/$overlapnum/${overlapnum}.out ] ; then
##  echo "${overlapnum}.out file already in dCache."
##    else
##  cp /data/des50.b/data/BLISS/${overlapnum:0:4}00/${overlapnum}/${overlapnum}.out /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$overlapnite/$overlapnum/
    DOTOUTFILES="${DOTOUTFILES} /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$overlapnite/$overlapnum/${overlapnum}.out" 
##    fi
    if [ $ALLGOOD == "true" ] ; then continue ; fi
fi

    #### at this point we have determined that we need to run SE proc for this exposure. so let's add it to the dag:

    # make sure that the directory for the raw image exists and has the appropriate permissions
    if [ ! -d /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/ ]; then
	mkdir /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/
	chmod 775  /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/
    fi

    # check if the raw image is present so that the SE processing can run. If it isn't, try to pull it over from des30.b, des51.b, NCSA DESDM, NOAO archive		
    if [ -e /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz ]; then
	echo "Raw image present in dCache"
    else	    
	if [ -e /data/des30.b/data/DTS/src/${overlapnite}/src/DECam_`printf %08d ${overlapnum}`.fits.fz ]; then
	    echo "Raw image not present in dcache; transferring from /data/des30.b"		
	    $COPYCMD /data/des30.b/data/DTS/src/${overlapnite}/src/DECam_`printf %08d ${overlapnum}`.fits.fz /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz || { echo "cp failed!" ; exit 2 ; }
	else 
	    if [ -e /data/des51.b/data/DTS/src/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz ]; then
		echo "Raw image not present in dCache or /data/des30.b; trying from des51.b"
		$COPYCMD /data/des51.b/data/DTS/src/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz || { echo "cp failed!" ; exit 2 ; }
	    else 
		echo " Raw image for exposure $overlapnum not in dcache and not in /data/des30.b or /data/des51.b. Try to tansfer from NCSA..."
		export WGETRC=$HOME/.wgetrc-desdm
		if [ ! -f $WGETRC ] ; then echo "Warning: Missing file $HOME/.wgetrc-desdm may cause wget authentication error." ; fi
		wget -nv https://desar2.cosmology.illinois.edu/DESFiles/desarchive/DTS/raw/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz 
		if [ $? -eq 0 ] ; then
		    $COPYDCMD DECam_`printf %08d ${overlapnum}`.fits.fz /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/ && rm DECam_`printf %08d ${overlapnum}`.fits.fz
		else
		    echo "wget failed! We will just skip this template."			

	#		    echo "wget failed! Will try to get image $overlapnum $overlapnite from NOAO."			
#		    fetch_noao
#		    if [ $? -ne 0 ]; then
#			echo "Failure in fetching from NOAO!"
#			if [ $i == 1 ] ; then echo "Cannot proceed without the search image!" ; exit 2 ; fi
			SKIP=true
#			echo "Unable to find raw image for overlapping exposure: $overlapnum ; will try to proceed without it."
			continue
#		    fi
		fi
	    fi
	fi
    fi

    echo "jobsub -n --group=des --OS=SL6 --resource-provides=usage_model=${RESOURCES} $JOBSUB_OPTS $JOBSUB_OPTS_SE file://SE_job.sh -r $RNUM -p $PNUM -E $overlapnum -b $BAND -n $overlapnite $JUMPTOEXPCALIBOPTION -d $DESTCACHE -m $SCHEMA $SE_OPTS" >> $outfile
### KH hack 2017-02-18
    echo "submitting job for $overlapnum"
#jobsub_submit --role=DESGW --group=des --OS=SL6 --resource-provides=usage_model=${RESOURCES} $JOBSUB_OPTS $JOBSUB_OPTS_SE file://SE_job.sh -r $RNUM -p $PNUM -E $overlapnum -b $BAND -n $overlapnite $JUMPTOEXPCALIBOPTION -d $DESTCACHE -m $SCHEMA $SE_OPTS

####################

    # add the .out file for this overlap image to the list to be copied
    DOTOUTFILES="${DOTOUTFILES} /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$overlapnite/$overlapnum/${overlapnum}.out"

done # end of loop over list of overlapping exposures

echo "end of loop over list of overlapping exposures"

# close the SE portion of the dag
echo "</parallel>" >> $outfile

#echo "To submit this DAG do"
#echo "jobsub_submit_dag -G des --role=DESGW file://${outfile}"


