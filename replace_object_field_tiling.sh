#!/bin/bash

#EXPS="606596 606583 606880 607172 607826 607839"
#EXPS=`cat templates_to_fix_headers.list`
#EXPS="667124 667144 667152 667168 667173 667175 667181 667185 667187 667208 667584 667590 667591 667600 667645 667647 667649 667650 667651 667653 667657 667658 667663 667669 667673 667674 667675 667676 667677 667678 667679 667680 667681 667682 667683 667684 667685 667686 667687 667688 667689 667690 668083 668085 668086 668087 668088 668089 668090 668091 668092 668093 668094 668095 668096 668097 668098 668099 668100 668101 668102 668103 668104 668105 668106 668107 668108 668109 668110 668111 668112 668113 668114 668115 668116 668117 668118 668119 668120 668121 668122 668123 668124 668125 669509 669512 669515 669546 669547 669570 669608 670868 670884 670892 670896 670901 670902 670904 670912 670917 670927 670928 670929 670930 670931 670932 670933 670934 670935 670936 670937 670938 670939 670940 670941 670953 671226 671227 671626 671627 671629 671631 671633 671637 671638 671657 671665 671667 671674 671675 672046 672054 672055 672060 672062 672064 672065 672067 672069 672070 672071 672072 672073 672074 672085 672086 672092 672094"

#EXPS="693612 693613 693614 693615 693616 693617"
#EXPS="693845 693846 693847 693848 693849 693850"
#EXPS="694529 694530 694531 694532 694533 694534"
#EXPS="668387 668388 668389"
#EXPS="856783 856786 856787 856788 856789 856790 856792 856793 856794 856795 856797 856798 856800 856801 856803 856804 856805 856806 856807 856808 856809 856810 856811 856813 856814"
#EXPS="879952 879954 879956 879958 879960 879962 879964 879966 879968 879970 879947 879949 879951 879953 879955 879957 879959 879961 879963 879965 879967 879969"
EXPS="938512 938513 938514 938515 938516 938517 938518 938519 938520 938521 938522 938523 938524 938525 938526 938527 938528 938529 938530 938531 938532 938533 938534 938535 938536 938537 938538 938539 938540 938541 938542 938543 938544 938545 938546" #still need 938511

RPNUM=r4p7

SCHEMA="gw"
DESTCACHE="persistent"

. /cvmfs/des.opensciencegrid.org/eeups/startupcachejob31i.sh 
setup wcstools
setup ftools v6.17
export PFILES=${HOME}/syspfiles

export IFDH_XROOTD_EXTRA="-f --silent -N -s"
export IFDH_FORCE="xrootd"
export IFDH_CP_MAXRETRIES=2
export EXPERIMENT=des

for exp in $EXPS
do
    
    NEWTILING=1
    imageline=$(awk '($1=='${exp}') {print $4,$5,$6,$2}' exposures.list )
    SEARCHRA=`echo $imageline | cut -d " " -f 1`
    SEARCHDEC=`echo $imageline | cut -d " " -f 2`
    band=`echo $imageline | cut -d " " -f 3`
    NITE=`echo $imageline | cut -d " " -f 4`
    RA10=$(echo "${SEARCHRA}*10" | bc | cut -d "." -f 1)
    DEC10=$(echo "$SEARCHDEC * 10" | bc | cut -d "." -f 1)
    if [ $DEC10 -ge 0 ]; then 
	DEC10="+${DEC10}"
    fi
    NEWFIELD="WS${RA10}${DEC10}"
    echo $NEWFIELD $NEWTILING $band $NITE
    NEWOBJECT="DESWS hex $NEWFIELD tiling $NEWTILING"
    echo "OBJECT = '${NEWOBJECT}' / Object name" > editfile
    echo "FIELD = '${NEWFIELD}'" >> editfile
    echo "TILING = 1" >> editfile

    # Account for expnum over 1000000
    exp8 = `printf %08d ${exp}`
    
    for ccd in 01 03 04 05 06 07 08 09 {10..60} 62
    do
	
	cp /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${exp}/D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz ./ || { echo "Error copying input file" ; continue ; }
#	sethead -x 0 D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz TILING=$NEWTILING || { echo "Error setting new tiling value for D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz" ; continue ; }
	
#	sethead -x 0 D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz OBJECT="${NEWOBJECT}" || { echo "Error setting new object value for D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz" ; continue ; }
#	sethead -x 0 D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz FIELD="${NEWFIELD}" || { echo "Error setting new field value for D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz" ; continue ; }
	fthedit D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz @editfile || { echo "Error running fthedit for D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz" ; continue ; } 
	#remove existing file in dache
	rm -f /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${exp}/D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz
	# only remove the local file if the copy to dcache succeeds
	cp  D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${exp}/ && rm D${exp8}_${band}_${ccd}_${RPNUM}_immask.fits.fz
	
    done
    rm editfile
done
