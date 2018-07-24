#!/bin/bash
                                                                             
if [ ! -f ~/.pgpass ]; then
    echo "You might have some problems running this because you don't have a .pgpass in your home directory. Try making one if
     if this doesn't work."
fi


source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh

setup easyaccess
setup psycopg2
python getExposureInfo.py

for band in u g r i z Y
do
awk '($6 == "'$band'")' exposures.list > exposures_${band}.list
done

exit
