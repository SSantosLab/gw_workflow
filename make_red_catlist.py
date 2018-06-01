#!/usr/bin/env python
import os
import string
import fitsio
import glob



class MakeRCat():
    
    def __init__(self ):
        print 'Start with make_red_catlist.py \n'
        self.file_list = glob.glob('*_immask.fits')
        self.sort_list = sorted(self.file_list)
        templ = self.sort_list[0]
        tokens = string.split(templ,'_')
        self.csvfile = tokens[0]+'_'+tokens[3]+'_red_catlist.csv'
        print 'creating %s \n' % self.csvfile
        if os.path.exists(self.csvfile):
                    os.remove(self.csvfile)
        self.outf = open(self.csvfile,'w')
        keywords=['EXPNUM', 'FILENAME', 'NITE', 'BAND', 'CCDNUM', 'AIRMASS', 'EXPTIME',
                   'RA_CENT', 'DEC_CENT', 'RAC1', 'DECC1', 'RAC2', 'DECC2', 'RAC3', 'DECC3', 'RAC4', 'DECC4']
        hstr = ''
        for tok in keywords:
            hstr+=tok+','
        hstr = hstr[:-1]
        self.outf.write(hstr+'\n')
    def runIt(self):
        for imf in self.sort_list:
            tokens = string.split(imf,'_')
            subtok = string.split(tokens[3],'.')[0]
            filename = tokens[0]+'_'+tokens[1]+'_'+tokens[2]+'_'+subtok+'_fullcat.fits'
            fits1 = fitsio.FITS(imf,'r')
            imhdr = fits1[0].read_header()
            try:
                expnum = str(imhdr['EXPNUM'])
            except:
                print " EXPNUM is not defined \n"
            try:            
                nite = str(imhdr['NITE'])
            except:
                print " NITE is not defined \n"
                nite = ''
            try:
                band = string.rstrip(str(imhdr['BAND']),' ')
            except:
                print " Band is not defined \n"
                band='g'
            try:
                ccdnum = str(imhdr['CCDNUM'])
            except:
                print "CCDNUM is not defined \n"
                ccdnum='01'
            try:
                airmass = str(imhdr['AIRMASS'])
            except:
                airmass = '1.3'
                print "airmass is not set will put 1.3 \n"
            try:
                exptime = string.split(str(imhdr['EXPTIME']),'.')[0]+'.'
            except:
                print " exptime is not set \n"
                exptime = '90.'
            ra_cent = str(imhdr['RA_CENT'])
            dec_cent = str(imhdr['DEC_CENT'])
            rac1 = str(imhdr['RAC1'])
            decc1 = str(imhdr['DECC1'])
            rac2 = str(imhdr['RAC2'])
            decc2 = str(imhdr['DECC2'])
            rac3 = str(imhdr['RAC3'])
            decc3 = str(imhdr['DECC3'])
            rac4 = str(imhdr['RAC4'])
            decc4 = str(imhdr['DECC4'])
            rec = expnum+','+filename+','+nite+','+band+','+ccdnum+','+airmass+','+exptime+','
            rec += ra_cent+','+dec_cent+','+rac1+','+decc1+','+rac2+','+decc2+','+rac3+','+decc3+','
            rec += rac4+','+decc4+'\n'
            self.outf.write(rec)
            fits1.close()
        self.outf.close()
        print 'Finish with make_red_catlist.py \n'
        
if __name__ == "__main__":
    mrc = MakeRCat()
    mrc.runIt()