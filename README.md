# gw_workflow
Workflow for image processing. Currently consists of single-epoch (SE) processing and difference imaging.

[Demo](#demo)

### Demo

##### Running `SE_job.sh` (or the modified `SE_job_mod.sh` with `-c` flag)

This script should be run on the cluster machines. SSH into a machine and make sure you have a proxy. This can be done as follows:
```
kx509   
voms-proxy-init -rfc -noregen -voms des:/des/Role=Analysis -valid 24:00
```

Then type `source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh`. You are now ready to run this script! As these jobs tend to take a long time, you may want to run it in Screen. This can be done by simply typing `screen`.

To run the script, type, for example,
```
./SE_job.sh -r 4 -p 5 -E 668439 -b i -n 20170817  -d persistent -m gw -C -O
```
*Note:* `SE_job.sh` and `SE_job_mod.sh` execute the script `BLISS-expCalib_Y3apass.py`. Due to deprecated package versions (should be fixed soon!), the `SE_job` scripts must run the deprecated version of the BLISS script called `BLISS-expCalib_Y3apass-old.py`. When running the BLISS script on its own, however, use the new version, `BLISS-expCalib_Y3apass.py`.  

##### Flags
* `r`: RNUM
* `p`: PNUM
* `E`: Exposure number
* `b`: Band (`i`, `r`, `g`, `Y`, `z`, or `u`)
* `n`: Night
* `c`: CCD list (comma-separated integers) -- for `SE_job_mod.sh` only
* `d`: Destination cache (`scratch` or `persistent`)
* `m`: Schema (`gw` or `wsdiff`)

The following flags do not require arguments:
 
* `C`: Turns on calibration
* `j`: Turns on `JUMPTOEXPCALIB`
* `s`: Runs single-threaded
* `Y`: Turns on `SPECIALY4`
* `O`: Fetches all files again, overwriting those already in the current directory
* `h`: Displays usage
