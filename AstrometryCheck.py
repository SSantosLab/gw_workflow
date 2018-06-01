#!/usr/bin/env python
#
# A simple program to check sanity of the astrometric solutions before
# start of expCalib. The program simply checks that all corners of all CCDs
# are within 1 degree 
#

import sys
import math
import string



class AstrometryCheck():
    
    def __init__(self,catlistFile):
        self.catlist = catlistFile
        self.ra_cent = []
        self.dec_cent = []
        self.rac1 = []
        self.decc1 = []
        self.rac2 = []
        self.decc2 = []
        self.rac3 = []
        self.decc3 = []
        self.rac4 = []
        self.decc4 = []
        first = True
        for line in open(self.catlist):
            if first:
                first = False
                continue
            tokens = string.split(line,',')
            self.ra_cent.append(float(tokens[7]))
            self.dec_cent.append(float(tokens[8]))
            self.rac1.append(float(tokens[9]))
            self.decc1.append(float(tokens[10]))
            self.rac2.append(float(tokens[11]))
            self.decc2.append(float(tokens[12]))
            self.rac3.append(float(tokens[13]))
            self.decc3.append(float(tokens[14]))
            self.rac4.append(float(tokens[15]))
            self.decc4.append(float(tokens[16]))
            
    def checkDiff(self):
        ra_cent_min = min(self.ra_cent)
        ra_cent_max = max(self.ra_cent)
        dec_cent_min = min(self.dec_cent)
        dec_cent_max = max(self.dec_cent)
        min_rac1 = min(self.rac1)
        max_rac1 = max(self.rac1)
        min_decc1 = min(self.decc1)
        max_decc1 = max(self.decc1)
        min_rac2 = min(self.rac2)
        max_rac2 = max(self.rac2)
        min_decc2 = min(self.decc2)
        max_decc2 = max(self.decc2)
        min_rac3 = min(self.rac3)
        max_rac3 = max(self.rac3)
        min_decc3 = min(self.decc3)
        max_decc3 = max(self.decc3)
        min_rac4 = min(self.rac4)
        max_rac4 = max(self.rac4)
        min_decc4 = min(self.decc4)
        max_decc4 = max(self.decc4)
        diff_racent = math.fabs(ra_cent_max - ra_cent_min)
        diff_deccent = math.fabs(dec_cent_max - dec_cent_min)
        diff_rac1 = math.fabs(max_rac1 - min_rac1)
        diff_decc1 = math.fabs(max_decc1 - min_decc1)
        diff_rac2 = math.fabs(max_rac2 - min_rac2)
        diff_decc2 = math.fabs(max_decc2 - min_decc2)
        diff_rac3 = math.fabs(max_rac3 - min_rac3)
        diff_decc3 = math.fabs(max_decc3 - min_decc3)
        diff_rac4 = math.fabs(max_rac4 - min_rac4)
        diff_decc4 = math.fabs(max_decc4 - min_decc4)

        if diff_deccent >= 4.0:
            return -1
        elif diff_decc1 >= 4.0:
            return -1
        elif diff_decc2 >= 4.0:
            return -1
        elif diff_decc3 >= 4.0:
            return -1
        elif diff_decc4 >= 4.0:
            return -1
        else:
            return 0       
if __name__ == "__main__":
    print sys.argv
    nbpar = len(sys.argv)
    infile = sys.argv[1]    
    print "Sanity check with %s \n" % infile  
    astrT = AstrometryCheck(infile)
    if  astrT.checkDiff() < 0:
        sys.exit(-1)
    else:
        sys.exit(0) 