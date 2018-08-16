#!/bin/bash 

#call as:  getcorners.sh $EXPNUM $DATADIR $CORNERDIR [ $CCDNUM ]

if [ $# -eq 3 ]; then
    echo "three arguments were passed; no ccdnum"

    # assign arguments
    CORNERDIR=$3
    DATADIR=$2
    e=$1

    outfile=${e}.out
    immaskfiles=$(ls ${DATADIR}/D$(printf %08d ${e})*_immask.fits.fz)
    # prefer .fz files, but if none found, try .fits file
    if [ -z $immaskfiles ]; then
	immaskfiles=$(ls ${DATADIR}/D$(printf %08d ${e})*_immask.fits)
    fi
elif [ $# -eq 4 ]; then
    echo "four arguments were passed; running with ccdnum"

    # reassign arguments
    CCD=$4
    CORNERDIR=$3
    DATADIR=$2
    e=$1

    outfile=${e}_${CCD}.out
    immaskfiles=$(ls ${DATADIR}/D$(printf %08d ${e})*_$(printf %02d ${CCD})*_immask.fits.fz)
    if [ -z "$immaskfiles" ]; then
	immaskfiles=$(ls ${DATADIR}/D$(printf %08d ${e})*_$(printf %02d ${CCD})*_immask.fits)
    fi
fi

AWK=/bin/awk

get_corners ()
{
  echo Getting corner coordinates for exposure ${thisinfo[$ii]} ...
  rm -f ${CORNERDIR}/$outfile

  for f in $immaskfiles
  #for f in ${DATADIR}/DECam_${e}/DECam*[0-9][0-9].fits
  do

    echo f is $f
    filt=`gethead FILTER ${f} | cut -c1`
    ccd=`gethead CCDNUM ${f}`

    echo "16 16" > cornerxy.dat
    echo "16 4081" >> cornerxy.dat
    echo "2033 16" >> cornerxy.dat
    echo "2033 4081" >> cornerxy.dat
    ${WCSTOOLS_DIR}/bin/xy2sky -d ${f} @cornerxy.dat > tmp.tmp${e}
    coord=( `${AWK} '{printf "%10.5f %10.5f  ",$1,$2}' tmp.tmp${e}` )
    # output = ( Expo Band CCD RA1 Dec1 RA2 Dec2 RA3 Dec3 RA4 Dec4 )
    echo ${e} ${filt} ${ccd} ${coord[0]} ${coord[1]} ${coord[2]} ${coord[3]} ${coord[4]} ${coord[5]} ${coord[6]} ${coord[7]} | \
      ${AWK} '{printf "%6d   %s   %2d  %10.5f %10.5f  %10.5f %10.5f  %10.5f %10.5f  %10.5f %10.5f\n",$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11}' >> ${CORNERDIR}/$outfile

  done
  rm -f tmp.tmp${e}
  hascorners[$i]=1
}


get_corners $1 $2
#ifdh cp -D 
