#!/bin/tcsh -f


# Same as getOverlaps.csh but doesn't separate into bands

# run trivalAccess to create the following file

# remember that the input file should be band-appropriate
set infile = $1
rm -f KH_diff_RADEC.tmp1 KH_diff_RADEC.tmp2
    
if ( $# < 1 ) then
    echo "Error, input file not specified. Exiting." 
    exit 1
endif

if ( ! -e $infile ) then
    echo "Error, $infile does not exist. Exiting."
    exit 1 
endif

foreach line ("`cat $infile`")

set myra = `echo $line | awk '{print $1}'`
set mydec = `echo $line | awk '{print $2}'`
set band =  `echo $line | awk '{print $3}'`

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
egrep -i -v "zero|flat|test|junk|bias|supernova|slew|pointing|commish|sn\-" exposures_${band}.list | \
  awk '{print $1,$2,$3,$4,$5,$6}' | \
  sort -k3 -gr | uniq > KH_RADEC.list

#remove the exposures in the blacklist
#foreach badexp ( `cat SE_blacklist.txt`)
#sed -i -r -e "/^${badexp}/d" KH_RADEC.list
#end

set dtor = `echo 45 | awk '{printf "%12.9f\n",atan2(1,1)/$1}'`
set ddeg = 2.2
set drad = `echo $ddeg | awk '{printf "%12.9f\n",cos($1*'$dtor')}'`


set nexp = `wc -l KH_RADEC.list | awk '{print $1}'`
#echo $nexp



set i = 1

set rar  = `echo $myra | awk '{printf "%9.6f",$1*'${dtor}'}'`
set decr = `echo $mydec | awk '{printf "%9.6f",$1*'${dtor}'}'`

#  while ($i <= 10)
    
  set expinfo = (`awk '(NR=='$i')' KH_RADEC.list`)

#  set band = $expinfo[6]

#  if ("$expinfo[1]" == "$myexp") then
#  echo $expinfo > KH_diff_RADEC.tmp0
#	awk '(NR>'${i}')&&($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad'){print $0,sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')}' KH_RADEC.list >> KH_diff_RADEC.tmp0


  # Output looks like this:
  # 4 i 271694 271688 270423 269353
  # 7 i 271692 271687 271684 271041 270421 269736 269353
  # 5 i 271691 271332 271032 270078 269737
  # 5 i 271690 271330 270776 270080 270079
  # 3 i 271688 271043 271034
  # ...
#  echo $band $expinfo[1] | awk '{printf "%s %6d",$1,$2}' >> KH_diff_RADEC.tmp1
  awk '($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad'){printf " %8d",$1}' KH_RADEC.list >> KH_diff_RADEC.tmp1
  echo hi | awk '{printf "\n"}' >> KH_diff_RADEC.tmp1


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
#  set ntempl = `wc -l KH_diff_RADEC.tmp0 | awk '{print $1-1}'`
#  awk '(NR==1){print $0,'${ntempl}'}' KH_diff_RADEC.tmp0 >> KH_diff_RADEC.tmp2
####   awk '($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad'){printf "%6d %8d %10.5f %12.9f\n",$1,$2,$3-'$expinfo[3]',atan2(sqrt(1-$7*$7),$7)/'${dtor}'}' KH_RADEC.list >> KH_diff_RADEC.tmp2
  awk '($6=="'${band}'")&&(sin('$decr')*sin($5*'${dtor}')+cos('$decr')*cos($5*'${dtor}')*cos('$rar'-$4*'${dtor}')>='$drad'){printf "%8d %8d %s\n",$1,$2,$6}' KH_RADEC.list >> KH_diff_RADEC.tmp2
  echo hi | awk '{printf "\n"}' >> KH_diff_RADEC.tmp2
#  break  
#endif


end

awk '{print NF-1,$0}' KH_diff_RADEC.tmp1 >> KH_diff_RADEC.list1
rm -f KH_diff_RADEC.tmp0 KH_diff_RADEC.tmp1
sort KH_diff_RADEC.tmp2 | uniq | sort -r > KH_diff_RADEC.list2
foreach exp ( `cat SE_blacklist.txt` )
sed -i -r -e "/^${exp}/d" KH_diff_RADEC.list2
end
rm -f KH_diff_RADEC.tmp2
