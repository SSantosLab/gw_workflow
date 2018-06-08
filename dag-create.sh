echo "<parallel>" > parallel.dag ; 
for i in 1 {3..9} {10..60} 62
do
    echo "jobsub -n -G des -M --memory=3500MB --disk=100GB --expected-lifetime=3h --resource-provides=usage_model=DEDICATED,OPPORTUNISTIC,OFFSITE --cpu=4 --append_condor_requirements='(TARGET.GLIDEIN_Site==\"FermiGrid\"||TARGET.HAS_CVMFS_des_opensciencegrid_org==true)&&(TARGET.CVMFS_des_osgstorage_org_REVISION>=38974)' file://SE_job_mod.sh -r 4 -p 5 -E 668439 -b i -n 20170817 -d persistent -m gw -C -O -c $i" >> parallel.dag
done 
echo "</parallel>" >> parallel.dag
