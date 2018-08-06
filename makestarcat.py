import numpy as np
import shutil
import sys
#from astropy.io import fits
import os.path
import esutil
import timeit
import argparse
import itertools
import os

start_time = timeit.default_timer()

#nite = 20170105
#expnum = 606868
#r = 2
#p = 11
#band = 'i'
#season = 300
#outdir = '/data/des41.b/data/rbutler/sb/bench/606868/'
#rawdir = '/pnfs/des/scratch/gw/dts/'
#rootdir = '/pnfs/des/persistent/gw/exp/'

envr = str(os.environ['RNUM'])
envp = str(os.environ['PNUM'])
envexp = str(os.environ['EXPNUM'])

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.\
RawDescriptionHelpFormatter)

parser.add_argument('-e', metavar='expnum', type=int, help='search image exposure number')

parser.add_argument('-n', metavar='nite', type=int, help='nite of search exposure')

parser.add_argument('-r', metavar='r', type=int, help='rnum for this run')

parser.add_argument('-p', metavar='p', type=int, help='pnum for this run')

parser.add_argument('-b', metavar='band', type=str, help='filter/band')

parser.add_argument('-s', metavar='season', type=int, help='season for this GW run')

parser.add_argument('--ccd', help='ccd is queried', default=1, type=int)

parser.add_argument('-outdir', type=str, default='./', help='directory where output files will go')

parser.add_argument('-rootdir', type=str, default='./', help='rootdir')

snstardef = 'SNSTAR_'+envexp+'_r'+envr+'p'+envp+'.LIST'
parser.add_argument('-snstar', type=str, default=snstardef, help='output SNSTAR filename')

snvetodef = 'SNVETO_'+envexp+'_r'+envr+'p'+envp+'.LIST'
parser.add_argument('-snveto', type=str, default=snvetodef, help='output SNVETO filename')

args = parser.parse_args()

dargs = vars(args)

expnum = args.e
nite = args.n
r = args.r
p = args.p
band = args.b
season = args.s
outdir = args.outdir
rootdir = args.rootdir
snstar = args.snstar
snveto = args.snveto

errct = 0
for key in dargs:
    if dargs[key]==None:
        errct = errct+1
        
if errct>0:
    print '-----------'
    print 'INPUT ERROR'
    print '-----------'
    print 'Failed to provide one of the following arguments:'
    print '-e (search image exposure number)'
    print '-n (nite of search exposure)'
    print '-r (rnum of current run)'
    print '-p (pnum of current run)'
    print '-b (filter/band of exposure)'
    print '-s (season for this GW run)'
    print '-outdir (directory where output files will go)'
    print '-rootdir (root directory up to the nite dir where copypairs.sh lives for this exposure, like /pnfs/des/persistent/gw/exp/)'
    sys.exit()

print "*** STARTING MAKESTARCAT ***"

#copypairs=rootdir+str(nite)+'/'+str(expnum)+'/dp'+str(season)+'/input_files/'+'copy_pairs_for_'+str(expnum)+'.sh'
copypairs='copy_pairs_for_'+str(expnum)+'.sh'

#if not os.path.isfile(copypairs):
#    print "copypairs input file",copypairs,"doesn't exist!"
#    sys.exit()

#hdulist = fits.open(impath)
# 
#imra = hdulist[0].header['RA']
#rasplit = imra.split(':')
#newra = 15*(float(rasplit[0])+(float(rasplit[1])/60.)+(float(rasplit[2])/3600.))
#maxra = newra+1.6
#minra = newra-1.6
#imdec = hdulist[0].header['DEC']
#decsplit = imdec.split(':')
#newdec = float(decsplit[0])-(float(decsplit[1])/60.)-(float(decsplit[2])/3600.)
#maxdec = newdec+1.1
#mindec = newdec-1.1
#
#hdulist.close()

dotout = str(expnum)+'.out'
outra1,outdec1,outra2,outdec2,outra3,outdec3,outra4,outdec4 = np.genfromtxt(\
    dotout,usecols=(3,4,5,6,7,8,9,10),unpack=True)
    
outra = list(itertools.chain(outra1,outra2,outra3,outra4))
outdec = list(itertools.chain(outdec1,outdec2,outdec3,outdec4))

maxra = max(outra)
minra = min(outra)
maxdec = max(outdec)
mindec = min(outdec)

over0= False
if maxra-minra>180:
    over0= True
    for o in range(len(outra)):
        if outra[o]>180:
            outra[o]=outra[o]-360
    maxra = max(outra)
    minra = min(outra)

#print maxra
#print minra

f = open(copypairs,'r')
line = f.readline()
f.close()

line = line.split()
del line[-1]
del line[:3]

pathlist=[]
for l in line:
    ll = l.split('/')
    y = ''
    for x in range(1,8):
        y = y + '/' + ll[x]
        if x==7:
            y = y + '/'
    pathlist.append(y)


def create(band):  
    global nite,expnum,r,p,maxra,minra,outra
    CATALOG,RA,DEC = [],[],[]
    MAG, ERRMAG = [],[]  
    cc = 0
    print "pathlist: %s" % pathlist
    for path in pathlist:
        cc=cc+1
        if path.split('/')[7]==str(expnum):
            continue
        else:
            psplit = path.split('/')
            exp = psplit[7]
            filename = 'D00'+str(exp)+'_r'+str(r)+'p'+str(p)+'_ZP.csv'
            filepath = path + filename
            print filepath
            #filepath = filename
            #if os.path.isfile(filename):
            #    hey = 0
            #    pass
            #else:
            #    hey = 1
            #    shutil.copy(filepath,'.')
            if os.path.isfile(filepath):
                ra,dec,mag_psf,magerr_psf,spread_model,flags,imaflags = np.genfromtxt(\
                    filepath,delimiter=',',skip_header=1,usecols=(4,5,12,13,14,20,21),unpack=True)
                print str(exp), 'succeeded.'
            else:
                print filepath, 'did not get copied.'        
                continue
            
            ra360 = []
            if over0:    
                for i in range(len(ra)):
                    if ra[i]>180:
                        ra360.append(ra[i]-360)
                    else:
                        ra360.append(ra[i])
            else:
                ra360 = ra           
            
            for i in range(len(ra)):
                print "spread_model[i]<0.003: %s -- flags[i]==0: %s -- imaflags[i]==0: %s -- mag_psf[i]>12, <21.5: %s -- magerr_psf[i]>0, >=0.011: %s -- minra<=ra360<=maxra: %s<=%s<=%s -- mindec<=dec[i]<=maxdec: %s<=%s<=%s" % (spread_model[i], flags[i], imaflags[i], mag_psf[i], magerr_psf[i], minra, ra360[i], maxra, mindec, dec[i], maxdec)
                if spread_model[i]<0.003 and flags[i]==0 and imaflags[i]==0\
                and mag_psf[i]>12 and mag_psf[i]<21.5 and magerr_psf[i]>0 and \
                magerr_psf[i]<=0.011 and minra<=ra360[i]<=maxra and mindec<=dec[i]<=maxdec:
                    cat = 'r'+str(r)+'p'+str(p)
                    CATALOG.append(cat)
                    RA.append(round(ra[i],6))
                    DEC.append(round(dec[i],6))
                    MAG.append(round(mag_psf[i],3))
                    ERRMAG.append(round(magerr_psf[i],5))
    
    #print "RA %s, DEC %s" % (RA, DEC)
    #print 'minra %.6f, maxra %.6f, mindec %.6f, maxdec %.6f' % (min(RA), max(RA), min(DEC), max(DEC))
    #print len(RA)
    #sys.exit()
            #print str(exp)
    RA,DEC,MAG,ERRMAG,CATALOG = zip(*sorted(zip(RA,DEC,MAG,ERRMAG,CATALOG)))
    elapsed = timeit.default_timer() - start_time
    mt, st = divmod(elapsed, 60)
    ht, mt = divmod(mt, 60)
    print "initial build complete %d:%02d:%02d" % (ht, mt, st)
    
    d = 1./3600.
    
    depth = 10
    
    print '------'
    lenra = len(RA)
    print 'total # of stars (incl. duplicates):',lenra
    print '------'
    
    h = esutil.htm.HTM(depth)
    #tt = 1./3600.
    elapsed = timeit.default_timer() - start_time
    mt, st = divmod(elapsed, 60)
    ht, mt = divmod(mt, 60)
    print "HTM complete %d:%02d:%02d" % (ht, mt, st)
    
    m1,m2,d12 = h.match(RA,DEC,RA,DEC,d,maxmatch=-1)
    elapsed = timeit.default_timer() - start_time
    mt, st = divmod(elapsed, 60)
    ht, mt = divmod(mt, 60)
    print "match complete %d:%02d:%02d" % (ht, mt, st)
    
    ra_new, dec_new, mag_new, errmag_new = [],[],[],[]
    listvals = []
    corres = []
    checker = []
    
    for i1 in sorted(list(set(m1))):
        prelim = sorted(np.where(m1 == i1)[0])
        if len(prelim)==1:
            ra_new.append(RA[i1])
            dec_new.append(DEC[i1])
            mag_new.append(MAG[i1])
            errmag_new.append(ERRMAG[i1])
            if i1%10000==0:
                elapsed = timeit.default_timer() - start_time
                mt, st = divmod(elapsed, 60)
                ht, mt = divmod(mt, 60)
                print "%i of %i" % (i1,lenra), ": %d:%02d:%02d" % (ht, mt, st)
        else:
            corres = []
            for pr in prelim:
                corres.append(m2[pr])
            if sorted(corres) in checker:
                if i1%10000==0:
                    elapsed = timeit.default_timer() - start_time
                    mt, st = divmod(elapsed, 60)
                    ht, mt = divmod(mt, 60)
                    print "%i of %i" % (i1,lenra), ": %d:%02d:%02d" % (ht, mt, st)
                continue
            else:
                checker.append(sorted(corres))
            indices,ara,adec,amag,aerrmag=[],[],[],[],[]
            listvals = corres
            for ind in listvals:
                ara.append(RA[ind])
                adec.append(DEC[ind])
                amag.append(MAG[ind])
                aerrmag.append(ERRMAG[ind])
            ra_new.append(np.mean(ara))
            dec_new.append(np.mean(adec))
            mag_new.append(np.median(amag))
            errsq = np.square(aerrmag)
            sumerrsq = np.sum(errsq)
            errmag_new.append((np.sqrt(sumerrsq))/float(len(aerrmag)))
            if i1%10000==0:
                elapsed = timeit.default_timer() - start_time
                mt, st = divmod(elapsed, 60)
                ht, mt = divmod(mt, 60)
                print "%i of %i" % (i1,lenra), ": %d:%02d:%02d" % (ht, mt, st)
    
    elapsed = timeit.default_timer() - start_time
    mt, st = divmod(elapsed, 60)
    ht, mt = divmod(mt, 60)
    print "final array build complete %d:%02d:%02d" % (ht, mt, st)
    
    #starcat = outdir+str(expnum)+'_STARCAT_modtest.LIST'
    starcat = outdir+snstar
    o = open(starcat, 'w+')
    o.write('# ASSOC format for Sextractor input.\n')
    o.write('# Star Source: SNSTAR table, CATALOG= r'+str(r)+'p'+str(p)+'\n')
    o.write('# NVAR: 5\n')
    o.write('# VARNAMES:\tOBJID\tRA\tDEC\tmag_'),o.write(band)
    o.write('\tmagerr_'),o.write(band),o.write('\n')
    
    ra_new, dec_new, mag_new, errmag_new = zip(*sorted(zip(ra_new, dec_new, mag_new, errmag_new)))
    
    idc = 0
    for y in range(len(ra_new)):
        idc = idc+1
        o.write(str(idc)),o.write('\t')
        o.write('%.6f' % ra_new[y]),o.write('\t')
        o.write('%.6f' % dec_new[y]),o.write('\t')
        o.write('%.4f' % mag_new[y]),o.write('\t')
        o.write('%.4f' % errmag_new[y]),o.write('\n')           
    o.close()
    
    print 'starcat written!',starcat
    
    ### VETO TABLE ###
    
    #starcatveto = outdir+str(expnum)+'_STARCAT_VETO_modtest.LIST'
    starcatveto = outdir+snveto
    v = open(starcatveto, 'w+')
    v.write('# ASSOC format for Sextractor input.\n')
    v.write('# Star Source: SNSTAR table, CATALOG= r'+str(r)+'p'+str(p)+'\n')
    v.write('# NVAR: 7\n')
    v.write('# VARNAMES:\tOBJID\tRA\tDEC\tmag_'),v.write(band)
    v.write('\tmagerr_'),v.write(band),v.write('\tradius\tvtype\n')
    
    idc = 0
    radius = []
    for y in range(len(ra_new)):
        idc = idc+1
        v.write(str(idc)),v.write('\t')
        v.write('%.6f' % ra_new[y]),v.write('\t')
        v.write('%.6f' % dec_new[y]),v.write('\t')
        v.write('%.4f' % mag_new[y]),v.write('\t')
        v.write('%.4f' % errmag_new[y]),v.write('\t')
        rad = 30.-2.*(mag_new[y]-15.)
        radius.append(rad) 
        v.write('%.6f' % radius[y]),v.write('\t')
        v.write(str(6)),v.write('\n')
    v.close()
    
    print 'veto table written!',starcatveto
    
create(band)
