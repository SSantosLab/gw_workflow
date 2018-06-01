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
    echo "usage: SEdiff.sh -E EXPNUM -r RNUM -p PNUM -P DIFFPROCNUM -n NITE -b BAND -N CCDNUM [-c CCDS] [-j] [-s] [-V SNVETO_NAME ] [-v DIFFIMG_VERSION] [-T STARCAT_NAME] [-m SCHEMA (gw or wsdiff)] [-Y] [-C] [-d destcache]" 
    exit 1
fi

procnum=r${RNUM}p$PNUM
OLDHOME=$HOME
export HOME=$PWD
DESTCACHE="persistent"
SCHEMA="wsdiff"
ulimit -a
OVERWRITE=false
CCDS=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62
#IFDHCP_OPT="--force=xrootd"
IFDHCP_OPT=""
DOCALIB="false"
FAILEDEXPS=""
#rpnum="r1p1"
#procnum="dp91"
DIFFIMG_VERSION="gwdevel13" # can change this with parameter -v <diffimg_version>
FULLCOPY=false


##mkdir joblib-0.9.0b4
##ifdh cp -r  /pnfs/des/scratch/marcelle/joblib-0.9.0b4 ./joblib-0.9.0b4
##export PYTHONPATH=$PYTHONPATH:$PWD/joblib-0.9.0b4

# get some worker node information
echo "Worker node information: `uname -a`"

which xrdcp >/dev/null 2>&1
CHECK_XRDCP=$?
which uberftp >/dev/null 2>&1
CHECK_UBERFTP=$?

# pretend that CHECK_XRDCP failed if we detect version 4.6.0 or 4.7.0 since they are buggy
XRDCP_VERSION=`xrdcp --version 2>&1`
if [ [[ $XRDCP_VERSION == *4.6.0* ]] || [[ $XRDCP_VERSION == *4.7.0* ]] ] ; then CHECK_XRDCP=1 ; fi

if [ $CHECK_XRDCP -ne 0 ] || [ $CHECK_UBERFTP -ne 0 ]; then
    if [ -f /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.4/current/el6-x86_64/setup.sh ]; then
    . /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.4/current/el6-x86_64/setup.sh
    else
	"Cannot find OASIS CVMFS setup file, and xrdcp and/or uberftp are not in the path."
    fi
fi

# 2018-03-09 replace with current OSG stack and 3.4 
. /cvmfs/oasis.opensciencegrid.org/mis/osg-wn-client/3.4/current/el6-x86_64/setup.sh

source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh
export IFDH_CP_MAXRETRIES=2
# export IFDH_XROOTD_EXTRA="-f -N"
export IFDH_XROOTD_EXTRA="-S 4 -f -N"
export XRD_REDIRECTLIMIT=255
export IFDH_CP_UNLINK_ON_ERROR=1

##### Don't forget to shift the args after you pull these out #####
ARGS="$@"
# we need to pull out expnum, chip, and band : KRH needs to double check syntax here
while getopts "E:n:b:r:p:S:d:c:V:v:T:CjhsYOFm:" opt $ARGS # S,V,T added from verifySE.sh
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
                    echo "Error: band option must be one of r,i,g,Y,z,u. You put $OPTARG."
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
    V)
	    SNVETO_NAME=$OPTARG
	    shift 2
	    ;;
    v) # from RUN_DIFFIMG
	    DIFFIMG_VERSION=$OPTARG
	    shift 2

	    ;;
    T)
	    STARCAT_NAME=$OPTARG
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
	    echo "usage: SEdiff.sh -E EXPNUM -r RNUM -p PNUM -n NITE -b BAND -S SEASON -N CCDNUM [-c CCDS] [-j] [-s] [-m gw|wsdiff] [-Y] [-C] [-O] [-d scratch|persistent] [-V snveto filename] [-v diffimg version] [-T starcat filename]"
	    exit 1
            ;;
	c)  # TODO: argument checking
		# usage: comma-separated list of CCDs
		CCDS=$OPTARG
		shift 2
		;;
    N) # from RUN_DIFFIMG
	    [[ $OPTARG =~ ^[0-9]+$ ]] || { echo "Error: CCD number must be an integer! You put $OPTARG" ; exit 1; }
	    [[ $OPTARG -lt 70 ]] || { echo "Error: the chip number must be less than 70. You entered $OPTARG." ; exit 1; }  
	    CCDNUM_LIST=$OPTARG
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
