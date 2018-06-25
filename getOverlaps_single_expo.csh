#!/bin/tcsh -f


# Same as getOverlaps.csh but doesn't separate into bands

# run trivalAccess to create the following file
#set allmyargs = "$@"
set infile = $1
set myexp = $2
set extraexps = `echo $argv | cut -d " " -f 3-`

if ( $# < 2 ) then
    echo "Error, you must specify an input exposure file as the first argument and then one or more search images. Exiting."
    exit 1
endif

# Output looks like this:
# 271694 20140104 56662.35752409 109.16375 -40.97922 i
# 271692 20140104 56662.35466475 108.479 -38.22839 i
# 271691 20140104 56662.35323273 108.46512 -33.79525 i
# 271690 20140104 56662.35172563 109.52162 -28.01675 i
# 271688 20140104 56662.34845991 110.15717 -42.56786 i
# ...

# new : allow supernova hex too
#grep " hex" ${infile} | \
#  awk '{print $2,$3,$4,$5,$6,$7}' | \

#grep "DES survey hex" ${infile} | \

egrep -i -v "zero|flat|test|junk|bias|supernova|slew|pointing|commish|sn\-" {$infile} | \
  awk '{print $1,$2,$3,$4,$5,$6}' | \
  sort -k3 -gr | uniq > KH.list

set dtor = `echo 45 | awk '{printf "%12.9f\n",atan2(1,1)/$1}'`
set ddeg = 2.2
set drad = `echo $ddeg | awk '{printf "%12.9f\n",cos($1*'$dtor')}'`


set nexp = `wc -l KH.list | awk '{print $1}'`
echo $nexp

rm -f KH_diff.tmp1 KH_diff.tmp2

set i = 1
while ($i <= $nexp)
#  while ($i <= 10)

    
  set expinfo = (`awk '(NR=='$i')' KH.list`)
  set rar  = `echo $expinfo[4] | awk '{printf "%9.6f",$1*'${dtor}'}'`
  set decr = `echo $expinfo[5] | awk '{printf "%9.6f",$1*'${dtor}'}'`
  set band = $expinfo[6]
  set mjd = $expinfo[3]
  if ("`grep $expinfo[1] ../SE_blacklist.txt`" == "") then

  if ("$expinfo[1]" == "$myexp") then

    echo $expinfo > KH_diff.tmp0

# check for any other args

if ( "x$extraexps" != "x" ) then
    foreach coadd ( ${extraexps} )
        if (`echo "$coadd > $myexp" | bc`) then
        awk '(NR<='${i}')&&($1=='${coadd}')&&($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad'){print $0,sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')}' KH.list >> KH_diff.tmp0
        endif
    end

    endif
    awk '(NR!='${i}')&&($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad')&&((($3-'$mjd')>=0.5)||(('$mjd'-$3)>=0.5)){print $0,sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')}' KH.list >> KH_diff.tmp0
#    awk '(NR>'${i}')&&($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad'){print $0,sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')}' KH.list >> KH_diff.tmp0


# Output looks like this:
# 4 i 271694 271688 270423 269353
# 7 i 271692 271687 271684 271041 270421 269736 269353
# 5 i 271691 271332 271032 270078 269737
# 5 i 271690 271330 270776 270080 270079
# 3 i 271688 271043 271034
# ...
echo $band $expinfo[1] | awk '{printf "%s %6d",$1,$2}' >> KH_diff.tmp1

# Now do a check for other coadds besides the first image
if ( "x$extraexps" != "x" ) then
    foreach coadd ( ${extraexps} )
        if (`echo "$coadd > $myexp" | bc`) then
        awk '(NR<='${i}')&&($1=='${coadd}')&&($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad'){printf " %6d",$1}' KH.list >> KH_diff.tmp1
        endif
    end

    endif
  awk '(NR!='${i}')&&($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad')&&((($3-'$mjd')>=0.5)||(('$mjd'-$3)>=0.5)){printf " %6d",$1}' KH.list >> KH_diff.tmp1
#  awk '(NR>'${i}')&&($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad'){printf " %6d",$1}' KH.list >> KH_diff.tmp1
echo hi | awk '{printf "\n"}' >> KH_diff.tmp1


  # Output looks like this:
  # 271694 20140104 56662.35752409 109.16375 -40.97922 i 3
  # 271688 20140104   -0.00906  1.752982109
  # 270423 20131231   -4.00808  1.573337661
  # 269353 20131228   -7.00588  1.355897213
  #
  # 271692 20140104 56662.35466475 108.479 -38.22839 i 6
  # 271687 20140104   -0.00770  0.818354351
  # 271684 20140104   -0.01279  2.175862002
  # 271041 20140102   -2.00095  2.137801761
  # 270421 20131231   -4.00864  1.929521714
  # 269736 20131229   -6.00556  0.849841514
  # 269353 20131228   -7.00302  1.507260058
  #
  # ...
set ntempl = `wc -l KH_diff.tmp0 | awk '{print $1-1}'`
  awk '(NR==1){print $0,'${ntempl}'}' KH_diff.tmp0 >> KH_diff.tmp2
  awk '(NR>1){printf "%6d %8d %10.5f %12.9f\n",$1,$2,$3-'$expinfo[3]',atan2(sqrt(1-$7*$7),$7)/'${dtor}'}' KH_diff.tmp0 >> KH_diff.tmp2
  echo hi | awk '{printf "\n"}' >> KH_diff.tmp2
  break  
endif
endif
  @ i++
end

awk '{print NF-1,$0}' KH_diff.tmp1 > KH_diff.list1
rm -f KH_diff.tmp0 KH_diff.tmp1
mv KH_diff.tmp2 KH_diff.list2
