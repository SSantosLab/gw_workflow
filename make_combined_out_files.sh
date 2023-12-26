#!/bin/bash

# assume args are list of nights, check for proper formatting
ARGS="$@"

# set vars (maybe make command line opts later, or source from dagmaker.rc)
DESTCACHE="persistent"
SCHEMA="gw"
BASEDIR="/pnfs/des/${DESTCACHE}/${SCHEMA}/exp"

for nite in $ARGS
do
    if [[ ! $nite =~ ^[0-9]{8}$ ]]; then
	echo "improper format for night. Skipping"
	continue
    fi
    exps=$(ls ${BASEDIR}/${nite})
    for exp in $exps
    do
	fulldir=${BASEDIR}/${nite}/${exp}
	if [ ! -s ${fulldir}/${exp}.out ]; then
	    nfiles=$(ls ${fulldir}/${exp}_*.out | wc -l)
	    if [ $nfiles -ge 60 ]; then
		cat ${fulldir}/${exp}_1.out ${fulldir}/${exp}_{3..9}.out ${fulldir}/${exp}_{10..60}.out ${fulldir}/${exp}_62.out > ${fulldir}/${exp}.out || echo "Error running cat for ${exp}."
	    elif [ $nfiles -eq 0 ]; then
		echo "No .out files present for $exp."
	    else
#		echo  "No combined.out file for ${exp} and one or more CCD .out files are missing; investigate."
		missingccds=""
		for ccd in 1 {3..9} {10..60} 62
		do
		    if [ ! -s ${fulldir}/${exp}_${ccd}.out ]; then
			missingccds="${missingccds} ${ccd}"
		    fi
		done
		echo "${exp} missing CCDs ${missingccds}."
	    fi
	fi
    done
done
