#!/bin/bash

if [ $# -lt 1 ]; then
    echo "usage: SE_job.sh -E EXPNUM -r RNUM -p PNUM -n NITE -b BAND [-c CCDS] [-j] [-s] [-O] [-m SCHEMA (gw or wsdiff)] [-Y] [-C] [-d destcache]" 
    exit 1
fi

OLDHOME=$HOME
export HOME=$PWD
DESTCACHE="persistent"
SCHEMA="gw"
ulimit -a
OVERWRITE=false
CCDS=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62

##testing a newer version of joblib
##mkdir joblib-0.9.0b4
##ifdh cp -r  /pnfs/des/scratch/marcelle/joblib-0.9.0b4 ./joblib-0.9.0b4
##export PYTHONPATH=$PYTHONPATH:$PWD/joblib-0.9.0b4

# get some worker node information
echo "Worker node information: `uname -a`"


which xrdcp
CHECK_XRDCP=$?
which uberftp
CHECK_UBERFTP=$?

# pretend that CHECK_XRDCP failed if we detect version 4.6.0 since it is buggy
XRDCP_VERSION=`xrdcp --version 2>&1`
if [[ $XRDCP_VERSION == *4.6.0* ]] ; then CHECK_XRDCP=1 ; fi

if [ $CHECK_XRDCP -ne 0 ] || [ $CHECK_UBERFTP -ne 0 ]; then
    if [ -f /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.3/current/el6-x86_64/setup.sh ]; then
	. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.3/current/el6-x86_64/setup.sh
    else
	"Cannot find CVMFS setup file, and xrdcp and/or uberftp are not in the path."
    fi
fi

. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.3/3.3.27/el6-x86_64/setup.sh
source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh
export IFDH_CP_MAXRETRIES=2
export IFDH_XROOTD_EXTRA="-f -N"
export XRD_REDIRECTLIMIT=255
export IFDH_CP_UNLINK_ON_ERROR=1

ARGS="$@"
while getopts "E:n:b:r:p:d:c:CjhsYOm:" opt $ARGS
do case $opt in
    E)
            [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: exposure number must be an integer! You put $OPTARG" ; exit 1; }
            EXPNUM=$OPTARG
            shift 2
            ;;
    n)
            [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: Night must be an integer! You put $OPTARG" ; exit 1; }
            NITE=$OPTARG
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
            RNUM=$OPTARG
            shift 2
            ;;
    p)
            PNUM=$OPTARG
            shift 2
            ;;
    j)
            JUMPTOEXPCALIB=true
            shift 
            ;;
    C)
            DOCALIB=true
            shift 
            ;;
    s)
            SINGLETHREAD=true
            shift 
            ;;
    d)
	    DESTCACHE=$OPTARG
	    shift 2
	    ;;
    m)
	    SCHEMA=$OPTARG
	    shift 2
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
	    echo "usage: SE_job.sh -E EXPNUM -r RNUM -p PNUM -n NITE -b BAND [-c CCDS] [-j] [-s] [-S scratch|persistent] [ -m gw|wsdiff] [-Y] [-C] [-O]" 
	    exit 1
            ;;
	c)  # TODO: argument checking
		# usage: comma-separated list of CCDs
		CCDS=$OPTARG
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

immaskfiles="`ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_r'${RNUM}'p'${PNUM}'_immask.fits.fz' | grep fits | grep fnal`"
nimmask=`echo $immaskfiles | wc -w`
if [ $nimmask -gt 59 ]; then
    psffiles="`ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_r'${RNUM}'p'${PNUM}'_fullcat.fits' | grep fits | grep fnal`" 
    npsf=`echo $psffiles | wc -w`
    if [ $npsf -gt 59 ]; then
	csvfiles="`ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'allZP_D*'${tempexp}'_r'${RNUM}p${PNUM}'*.csv' | grep fnal` `ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'Zero_*'${tempexp}'_r'${RNUM}p${PNUM}'*.csv' | grep fnal` `ifdh ls  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'D*'${tempexp}'_r'${RNUM}p${PNUM}'*_ZP.csv' | grep fnal`" 
	ncsv=`echo $csvfiles | wc -w`
	if [ $ncsv -ge 3 ]; then
	    if [ "$OVERWRITE" == "false" ]; then
		echo "All SE processing for $EXPNUM, r=$RNUM, p=$PNUM is complete. This job will now exit."
		exit 0
	    else
		echo "All files are present, but the -O option was given, so we are continuing on with the job."
	    fi
	else
	    echo "csv files not all present; continuing with the job."
	fi
    else
	echo "fullcat files not all present; continuing with the job."
    fi
else
    echo "immask files not all present; continuing with the job."
fi

if [ "$DOCALIB" == "true" ] ; then
    echo "This SE job will include GGG calib step."
else
    echo "This SE job will use calib info from the DB."
    filestorm="`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_r'${RNUM}p${PNUM}'_fullcat.fits' | grep fits | grep fnal` `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_r'${RNUM}p${PNUM}'*csv*' | grep csv | grep fnal`"
    if [ ! -z "${filestorm}" ]; then
	ifdh rm $filestorm
    fi
fi

#### add other code here from Nikolay's area

# ifdh cp -D /pnfs/des/persistent/desdm/code/desdmLiby1e2.py /pnfs/des/persistent/desdm/code/run_desdmy1e2.py /pnfs/des/persistent/desdm/code/run_SEproc.py  /pnfs/des/persistent/desdm/code/getcorners.sh /pnfs/des/persistent/kuropat/scripts/MySoft.tgz  /pnfs/des/scratch/gw/code/test_mysql_libs.tar.gz ./ || { echo "Error copying input files. Exiting." ; exit 2 ; }
# tar xzf ./MySoft.tgz # COMMENTED OUT on 6/4 to avoid recopying of BLISS file
<<<<<<< HEAD
=======
 tar xzf ./MySoft.tgz # COMMENTED OUT on 6/4 to avoid recopying of BLISS file
>>>>>>> tmp

ifdh cp --force=xrootd /pnfs/des/persistent/${SCHEMA}/db-tools/desservices.ini ${HOME}/.desservices.ini

tar xzfm ./test_mysql_libs.tar.gz

export DES_SERVICES=${HOME}/.desservices.ini
chmod 600 ${HOME}/.desservices.ini
chmod +x make_red_catlist.py BLISS-expCalib_Y3apass.py getcorners.sh

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
    headfile='heads/f'$CCDS'.head'
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
	headfile='heads/f'$CCDS'.head'
	pcaprefix='binned-fp/Y2A1_20130101t0315_{filter:s}_r1979p01_skypca-binned-fp.fits'	
    elif [ $EXPNUM -lt 226353 ]; then
	YEAR=sv
	EPOCH=e1
	biasfile='D_n20130115t0131_c{ccd:>02s}_r1788p01_biascor.fits'
	bpmfile='D_n20130115t0130_c{ccd:>02s}_r1975p01_bpm.fits'
	dflatfile='D_n20130115t0131_{filter:s}_c{ccd:>02s}_r1788p01_norm-dflatcor.fits'
	skytempfile='Y2A1_20130101t0315_{filter:s}_c{ccd:>02s}_r1979p01_skypca-tmpl.fits'
	starflatfile='Y2A1_20130101t0315_{filter:s}_c{ccd:>02s}_r1976p01_starflat.fits'
	headfile='heads/f'$CCDS'.head'
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
	headfile='heads/f'$CCDS'.head'
	pcaprefix='binned-fp/Y2A1_20130801t1128_{filter:s}_r2044p01_skypca-binned-fp.fits'
    elif [ $EXPNUM -lt 284391 ]; then
	YEAR=y1
	EPOCH=e2
	biasfile='D_n20140117t0129_c{ccd:>02s}_r2045p01_biascor.fits'
	bpmfile='D_n20140117t0129_c{ccd:>02s}_r2105p01_bpm.fits'
	dflatfile='D_n20140117t0129_{filter:s}_c{ccd:>02s}_r2045p01_norm-dflatcor.fits'
	skytempfile='Y2A1_20131129t0315_{filter:s}_c{ccd:>02s}_r2106p01_skypca-tmpl.fits'
	starflatfile='Y2A1_20131129t0315_{filter:s}_c{ccd:>02s}_r2107p01_starflat.fits'
	headfile='heads/f'$CCDS'.head'
	pcaprefix='binned-fp/Y2A1_20131129t0315_{filter:s}_r2106p01_skypca-binned-fp.fits'
    elif [ $EXPNUM -le 383321 ]; then
	YEAR=y2
	EPOCH=e1
	biasfile='D_n20141204t1209_c{ccd:>02s}_r1426p08_biascor.fits'
	bpmfile='D_n20141020t1030_c{ccd:>02s}_r1474p01_bpm.fits'
	dflatfile='D_n20141020t1030_{filter:s}_c{ccd:>02s}_r1471p01_norm-dflatcor.fits'
	skytempfile='Y2A1_20140801t1130_{filter:s}_c{ccd:>02s}_r1635p01_skypca-tmpl.fits'
	starflatfile='Y2A1_20140801t1130_{filter:s}_c{ccd:>02s}_r1637p01_starflat.fits'
        headfile='heads/f'$CCDS'.head'                                                 
	pcaprefix='binned-fp/Y2A1_20140801t1130_{filter:s}_r1635p01_skypca-binned-fp.fits'
    elif [ $EXPNUM -le 438444 ]; then
	YEAR=y2
	EPOCH=e2
	biasfile='D_n20150105t0115_c{ccd:>02s}_r2050p02_biascor.fits'
	bpmfile='D_n20150105t0115_c{ccd:>02s}_r2134p01_bpm.fits'
	dflatfile='D_n20150105t0115_{filter:s}_c{ccd:>02s}_r2050p02_norm-dflatcor.fits'
	skytempfile='Y2A1_20141205t0315_{filter:s}_c{ccd:>02s}_r2133p01_skypca-tmpl.fits'
	starflatfile='Y2A1_20141205t0315_{filter:s}_c{ccd:>02s}_r2132p01_starflat.fits'
        headfile='heads/f'$CCDS'.head'                                                 
	pcaprefix='binned-fp/Y2A1_20141205t0315_{filter:s}_r2133p01_skypca-binned-fp.fits'
    elif [ $EXPNUM -le 519543 ]; then
	YEAR=y3
	EPOCH=e1
	biasfile='D_n20151113t1123_c{ccd:>02s}_r2350p02_biascor.fits'
	bpmfile='D_n20151113t1123_c{ccd:>02s}_r2359p01_bpm.fits'
	dflatfile='D_n20151113t1123_{filter:s}_c{ccd:>02s}_r2350p02_norm-dflatcor.fits'
	skytempfile='Y2A1_20150715t0315_{filter:s}_c{ccd:>02s}_r2361p01_skypca-tmpl.fits'
	starflatfile='Y2A1_20150715t0315_{filter:s}_c{ccd:>02s}_r2360p01_starflat.fits'
        headfile='heads/f'$CCDS'.head'                                                 
	pcaprefix='binned-fp/Y2A1_20150715t0315_{filter:s}_r2361p01_skypca-binned-fp.fits'
    else
	YEAR=y4
	EPOCH=e1
	biasfile='D_n20151113t1123_c{ccd:>02s}_r2350p02_biascor.fits'
	bpmfile='D_n20151113t1123_c{ccd:>02s}_r2400p01_bpm.fits'
	dflatfile='D_n20151113t1123_{filter:s}_c{ccd:>02s}_r2350p02_norm-dflatcor.fits'
	skytempfile='Y2T4_20150715t0315_{filter:s}_c{ccd:>02s}_r2404p01_skypca-tmpl.fits'
	starflatfile='Y2A1_20150715t0315_{filter:s}_c{ccd:>02s}_r2360p01_starflat.fits'
        headfile='heads/f'$CCDS'.head'                                                 
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
        headfile='heads/f'$CCDS'.head'                                                 
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
    conf_dir="/pnfs/des/persistent/desdm/config/"
fi

cat <<EOF >> confFile
[General]
nite: 20141229
expnum: 393047
filter: z
r: 04
p: 11
chiplist: $CCDS
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

#sed -i -e "/^nite\:/ s/nite\:.*/nite\: ${NITE}/" -e "/^expnum\:/ s/expnum\:.*/expnum\: ${EXPNUM}/" -e "/^filter\:/ s/filter:.*/filter\: ${BAND}/" -e "/^r\:/ s/r:.*/r\: ${RNUM}/" -e "/^p\:/ s/p:.*/p\: ${PNUM}/" -e "/^year\:/ s/year:.*/year\: ${YEAR}/" -e "/^yearb\:/ s/yearb:.*/yearb\: ${YEAR}/" -e "/^epoch\:/ s/epoch:.*/epoch\: ${EPOCH}/"  -e "/^epochb\:/ s/epochb:.*/epochb\: ${EPOCH}/" confFile
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

if [ "$JUMPTOEXPCALIB" == "true" ] ; then
    echo "jumping to the calibration step..."
    nfiles=`ls *_r${RNUM}p${PNUM}_fullcat.fits *_r${RNUM}p${PNUM}_immask.fits.fz | wc -l`
    nccds=`grep chiplist confFile | awk -F ':' '{print $2}' | sed 's/,/ /g' | wc -w`
    nccds2=`expr $nccds \* 2`
    if [  $nfiles -ne $nccds2 ] ; then
	echo "copying fits files from Dcache"
	filestocopy1="`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_r'${RNUM}'p'${PNUM}'_fullcat.fits' | grep fnal | grep fits`"
	filestocopy2="`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'*_r'${RNUM}'p'${PNUM}'_immask.fits.fz' | grep fnal | grep fits`"
	ifdh cp --force=xrootd -D $filestocopy1 $filestocopy2 .
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
#    ./make_red_catlist.sh
    echo "make_red_catlist.py finished with exit status $?"

#### change to python version
    
  
#### copy to here the relevant calib files:
### No longer needed as of Dec 2016
###    ifdh cp --force=xrootd -D /pnfs/des/scratch/marcelle/apass_2massInDESplus2.sorted.csv .
#### for now, we only have it in the DES footprint. 
    
##### testing my own version of GGG-expCalib
#    ifdh cp --force=xrootd -D /pnfs/des/scratch/marcelle/GGG-expCalib_Y3apass.py . || echo "Error copying /pnfs/des/scratch/marcelle/GGG-expCalib_Y3apass.py"
#    chmod 775 ./GGG-expCalib_Y3apass.py
#    ./GGG-expCalib_Y3apass.py -s desoper --expnum $EXPNUM --reqnum $RNUM --attnum $PNUM
#switch to BLISS version

    unset healpy astropy fitsio matplotlib six python
    export CONDA_DIR=/cvmfs/des.opensciencegrid.org/fnal/anaconda2
    source $CONDA_DIR/etc/profile.d/conda.sh
    conda activate des18a
    
    ./BLISS-expCalib_Y3apass.py --expnum $EXPNUM --reqnum $RNUM --attnum $PNUM --ccd $CCDS
    
    RESULT=$? 
    echo "GGG-expCalib_Y3pass.py exited with status $RESULT"
    
#not needed anymore since we are going to overwrite existing files via xrootd    
#    files2rm="`ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'allZP*r'${RNUM}'*p'${PNUM}'*.csv' | grep csv` `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'Zero*r'${RNUM}'*p'${PNUM}'*.csv' | grep csv` `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'D*'${EXPNUM}'*_ZP.csv'  | grep csv` `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'D*'${EXPNUM}'*CCDsvsZPs.png' | grep png` `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'D*'${EXPNUM}'*NumClipstar.png' | grep png` `ifdh ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/'D*'${EXPNUM}'*ZP.png'  | grep png`" 
#    for rmfile in $files2rm ; do ifdh rm $rmfile || echo "ifdh rm of $rmfile failed, so the final copyback may not work." ; done

files2cp=`ls allZP*r${RNUM}*p${PNUM}*.csv Zero*r${RNUM}*p${PNUM}*.csv D*${EXPNUM}*_ZP.csv D*${EXPNUM}*CCDsvsZPs.png D*${EXPNUM}*NumClipstar.png D*${EXPNUM}*ZP.png`
if [ "x${files2cp}" = "x" ]; then
    echo "Error, no calibration files to copy!"
else
    ifdh cp --force=xrootd -D $files2cp /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/ || echo "ifdh cp of calibration csv and png files failed. There could be problems with Diffimg down the road when using this exposure."
fi
fi

du -sh .

# cleanup if we are in a grid job (defined as having the GRID_USER environment variable set) to avoid potential timeouts on exit
if [ -n "$GRID_USER" ] ; then rm -f *.fits *.fits.fz *.ps *.psf *.xml full_1.cat *.head ; fi
#rm *.csv *.png

export HOME=$OLDHOME


