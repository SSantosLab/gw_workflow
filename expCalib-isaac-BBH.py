#!/usr/bin/env python
"""
    expCalib.py
    Express Calibration
    This code will estimate the zero-points
    
    v.7 Mar06, 2024:
    Completely refactored by Isaac; Faster, Better, Stronger
    v.6 Jan03, 2023:
    Nora fixed astropy.stats.sigma_clip kwargs
    v.5 Jun01, 2018:
    Moved standard stars from APASS to GAIA
    v.4 Oct09, 2017:
    According to Sahar rounding was changed from 357 to 350.
    v.3 Apr20, 2016:
    NOW using apass_2massInDES.sorted.csv via APASS/2MASS.
    v.2 Feb25, 2016:
    Now use APASS Dr7 and tested with ALex.. MagsLite
    v.1 Sep24, 2015:
    NOTE that APASS is only for the officical DES-foot print, some ccd will have no ZP
    
    Example: expCalib.py --expnum 887849 --reqnum 4 --attnum 10 --ccd 37
"""
import os, sys, io, glob, argparse, requests
import numpy as np
import healpy as hp
import pandas as pd
import fitsio
from astropy.table import Table
from astropy.stats import sigma_clip
from astropy.io import fits
import matplotlib.pyplot as plt

# Create command line arguments
parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--expnum', help='expnum is queried', default=887849, type=int)
parser.add_argument('--reqnum', help='reqnum is queried', default=4, type=str)
parser.add_argument('--attnum', help='attnum is queried', default=10, type=int)
parser.add_argument('--ccd', help='ccd is queried', default=37, type=int)
parser.add_argument('--magType', help='mag type to use (mag_psf, mag_auto, mag_aper_8, ...)', default='mag_psf')
parser.add_argument('--sex_mag_zeropoint', help='default sextractor zeropoint to use to convert fluxes to sextractor mags (mag_sex = -2.5log10(flux) + sex_mag_zeropoint)', type=float, default=25.0)
parser.add_argument('--verbose', help='verbosity level of output to screen (0,1,2,...)', default=0, type=int)
parser.add_argument('--debug', help='debugging option', dest='debug', action='store_true', default=False)
args = parser.parse_args()

################################################################################

def getallccdfromDELVE(args, catlist_data, catfile, remove_agns=True):
    outfile = "STD%s" % catfile
    
    # Check image standard deviation 
    if np.std(catlist_data['RA_CENT']) > 20:
        catlist_data['RA_CENT'] = roundRA(catlist_data['RA_CENT'])
        catlist_data['RAC1'] = roundRA(catlist_data['RAC1'])
        catlist_data['RAC2'] = roundRA(catlist_data['RAC2'])
        catlist_data['RAC3'] = roundRA(catlist_data['RAC3'])
        catlist_data['RAC4'] = roundRA(catlist_data['RAC4'])
    band = catlist_data['BAND'][0]
    
    # Get image limits
    minra = np.min(catlist_data[['RA_CENT', 'RAC1', 'RAC2', 'RAC3', 'RAC4']].values) - .1
    mindec = np.min(catlist_data[['DEC_CENT', 'DECC1', 'DECC2', 'DECC3', 'DECC4']].values) - .1
    maxra = np.max(catlist_data[['RA_CENT', 'RAC1', 'RAC2', 'RAC3', 'RAC4']].values) + .1
    maxdec = np.max(catlist_data[['DEC_CENT', 'DECC1', 'DECC2', 'DECC3', 'DECC4']].values) + .1
    
    std_data = getCatalogDELVE(catlist_data['RA_CENT'][0], catlist_data['DEC_CENT'][0], minra, maxra, mindec, maxdec, band)
    if std_data is None:
        print 'Querying PanSTARRS instead...'
        std_data = getCatalogPanSTARRS(catlist_data['RA_CENT'][0], catlist_data['DEC_CENT'][0], minra, maxra, mindec, maxdec, band)
        if std_data is None:
            sys.exit(1)
    std_data.columns = ["MATCHID", "RA", "DEC", "WAVG_MAG_PSF"]
    
    if remove_agns:
        allwiseagns_df = pd.read_table("/cvmfs/des.osgstorage.org/pnfs/fnal.gov/usr/des/persistent/stash/gw/ALLWISE_AGN/allwiseagn_v1_082022.dat",
                                       sep=' ', names=['WISEA', 'RA', 'DEC', 'W1-W2', 'W2-W3', 'W1mag'])
        allwiseagns_df = allwiseagns_df[(allwiseagns_df['DEC']>mindec) & (allwiseagns_df['DEC']<maxdec)]
        allwiseagns_df = allwiseagns_df[(allwiseagns_df['RA']>minra) & (allwiseagns_df['RA']<maxra)]
        
        agn_coords = np.array([allwiseagns_df['RA'], allwiseagns_df['DEC']]).T
        std_coords = np.array([std_data['RA'], std_data['DEC']]).T
        
        match_tolerance = 1.0 # Arcsec
        coord_broadcast0 = np.repeat(std_coords[np.newaxis, :, :], len(agn_coords), axis=0)
        coord_broadcast1 = np.repeat(agn_coords[:, np.newaxis, :], len(std_coords), axis=1)
        min_sep = np.degrees(np.min(vectorized_angsep(coord_broadcast0, coord_broadcast1), axis=1))
        tolerance_mask = (min_sep < (match_tolerance/3600.)) # Matches should be within some tolerance criteria
        idx = np.argmin(vectorized_angsep(coord_broadcast0, coord_broadcast1), axis=1)
        idx = idx[tolerance_mask]
        
        std_data = std_data.drop(idx).reset_index(drop=True)
    
    std_data.to_csv(outfile, columns=["MATCHID", "RA", "DEC", "WAVG_MAG_PSF"], index=False)
    
    # Filtering image pixel set into CCD sets, per CCD
    for i in range(len(catlist_data)):
        # Calculate edges of image
        std_file = "%s_std.csv"   % (catlist_data['FILENAME'][i])
        minra = np.min(catlist_data[['RA_CENT', 'RAC1', 'RAC2', 'RAC3', 'RAC4']].iloc[i].values) - .1
        mindec = np.min(catlist_data[['DEC_CENT', 'DECC1', 'DECC2', 'DECC3', 'DECC4']].iloc[i].values) - .1
        maxra = np.max(catlist_data[['RA_CENT', 'RAC1', 'RAC2', 'RAC3', 'RAC4']].iloc[i].values) + .1
        maxdec = np.max(catlist_data[['DEC_CENT', 'DECC1', 'DECC2', 'DECC3', 'DECC4']].iloc[i].values) + .1
        
        bounds_mask = (std_data['RA'] > minra) & (std_data['RA'] < maxra) & (std_data['DEC'] > mindec) & (std_data['DEC'] < maxdec)
        #std_ccd_data = std_data[bounds_mask].sort_values(['RA'], ascending=True)
        std_ccd_data = std_data[bounds_mask].sort(['RA'], ascending=True)
        std_ccd_data.to_csv(std_file, columns=["MATCHID", "RA", "DEC", "WAVG_MAG_PSF"], index=False)
    return


def getCatalogDELVE(ra, dec, minra, maxra, mindec, maxdec, band, radius=0.2):
    #vec = hp.pixelfunc.ang2vec(ra, dec, lonlat=True) # For new healpy 1.11.0 and python 2.7.15
    vec = hp.pixelfunc.ang2vec((90-dec) * np.pi/180, ra * np.pi/180) # FOR OLD HEALPY 1.5dev
    disc_hpx = hp.query_disc(32, vec, radius=np.radians(radius), inclusive=True)
    print "{} pixels to be queried from DELVE_DR2".format(disc_hpx)
    band = band.upper()
    
    out_columns = ['QUICK_OBJECT_ID', 'RA', 'DEC', 'WAVG_MAG_PSF_'+band]
    cut_columns = ['WAVG_FLAGS_'+band, 'WAVG_SPREAD_MODEL_'+band, 'CLASS_STAR_'+band]
    
    data = pd.DataFrame(columns=out_columns)
    for hpx in disc_hpx:
        # Also at '/data/des91.b/data/kadrlica/projects/delve/cat/dr2/cat/cat_hpx_{0:05d}.fits'.format(hpx)
        filename = glob.glob('/cvmfs/des.osgstorage.org/pnfs/fnal.gov/usr/des/persistent/stash/gw/DELVE_DR2/delvedr2_*/cat_hpx_{0:05d}.fits'.format(hpx))
        if len(filename)!=0:
            print(filename[0])
            d = pd.DataFrame.from_records(fitsio.read(filename[0], columns=out_columns+cut_columns).byteswap().newbyteorder())
            mask = (d['RA']>minra) & (d['RA']<maxra) & (d['DEC']>mindec) & (d['DEC']<maxdec) & \
                    (d['WAVG_FLAGS_'+band]<=3) & (d['CLASS_STAR_'+band]>0.8) & \
                    (d['WAVG_SPREAD_MODEL_'+band]<0.01) & (d['WAVG_MAG_PSF_'+band]>0)
            d = d[mask]
            data = pd.concat([data, d[out_columns]], ignore_index=True)
    if len(data) == 0:
        print "No DELVE_DR2 coverage at this region."
        return None
    return data.reset_index(drop=True)


def getCatalogPanSTARRS(ra, dec, minra, maxra, mindec, maxdec, band, factor=3.5):
    if band in ['g', 'r']:
        query_bands = ['g', 'r']
    elif band in ['i', 'z']:
        query_bands = ['i', 'z']
    else:
        print 'Band not valid'
        return
    
    columns = ['objID', 'raMean', 'decMean', '{}MeanPSFMag'.format(query_bands[0]), '{}MeanPSFMag'.format(query_bands[1])]
    constraints = {'ra':ra, 'dec':dec, 'radius':np.sqrt((maxra - minra)**2 + (maxdec - mindec)**2)/factor,
                   'nDetections.gt':1, 'raMean.lt':maxra, 'raMean.gt':minra, 'decMean.lt':maxdec, 'decMean.gt':mindec}
    
    if columns:
        dcols = {}
        for col in ps1metadata('mean', 'dr2')['name']:
            dcols[col.lower()] = 1
        badcols = []
        for col in columns:
            if col.lower().strip() not in dcols:
                badcols.append(col)
        if badcols:
            raise ValueError('Some columns not found in table: {}'.format(', '.join(badcols)))
        constraints['columns'] = '[{}]'.format(','.join(columns))
        
    url = "https://catalogs.mast.stsci.edu/api/v0.1/panstarrs/dr2/mean.csv"
    r = requests.get(url, params=constraints)
    r.raise_for_status()
    if format == "json":
        result = r.json()
    else:
        result = r.text
    
    data = pd.read_csv(io.StringIO(result))
    data.columns = ['id', 'ra', 'dec', query_bands[0], query_bands[1]]
    
    converted = PanSTARRS2DECamMagTransformation(data, band)
    conv_mask = (converted!=-999.)
    
    data['converted'] = converted
    data = data[conv_mask]
    data = data[['id', 'ra', 'dec', 'converted']]
    
    data.columns = ['QUICK_OBJECT_ID', 'RA', 'DEC', 'WAVG_MAG_PSF_'+band.upper()]
    if len(data) == 0:
        print "No PanSTARRS coverage at this region."
        return
    return data.reset_index(drop=True)


def checklegal(table, release):
    """
    (FROM PANSTARRS API DOCUMENTATION http://ps1images.stsci.edu/ps1_dr2_api.html)
    Checks if this combination of table and release is acceptable
    Raises a ValueError exception if there is problem
    """
    
    releaselist = ("dr1", "dr2")
    if release not in ("dr1","dr2"):
        raise ValueError("Bad value for release (must be one of {})".format(', '.join(releaselist)))
    if release=="dr1":
        tablelist = ("mean", "stack")
    else:
        tablelist = ("mean", "stack", "detection")
    if table not in tablelist:
        raise ValueError("Bad value for table (for {} must be one of {})".format(release, ", ".join(tablelist)))


def ps1metadata(table="mean", release="dr1", baseurl="https://catalogs.mast.stsci.edu/api/v0.1/panstarrs"):
    """
    (FROM PANSTARRS API DOCUMENTATION http://ps1images.stsci.edu/ps1_dr2_api.html)
    Return metadata for the specified catalog and table
    
    Parameters
    ----------
    table (string): mean, stack, or detection
    release (string): dr1 or dr2
    baseurl: base URL for the request
    
    Returns an astropy table with columns name, type, description
    """
    
    checklegal(table, release)
    url = "{}/{}/{}/metadata".format(baseurl, release, table)
    r = requests.get(url)
    r.raise_for_status()
    v = r.json()
    # convert to astropy table
    tab = Table(rows=[(x['name'],x['type'],x['description']) for x in v], names=('name','type','description'))
    return tab


def PanSTARRS2DECamMagTransformation(ps_data, band):
    # Conversion values from Douglas Tucker and DELVE
    if band=='g':
        color = ps_data['g'].values - ps_data['r'].values
        convert = ps_data['g'].values + 0.0994 * color - 0.0076
        convert[(ps_data['g'].values<0)|(ps_data['r'].values<0)|(color<=-0.2)|(color>1.2)] = -999.
        return convert
    elif band=='r':
        color = ps_data['g'].values - ps_data['r'].values
        convert = ps_data['r'].values - 0.1335 * color + 0.0189
        convert[(ps_data['g'].values<0)|(ps_data['r'].values<0)|(color<=-0.2)|(color>1.2)] = -999.
        return convert
    elif band=='i':
        color = ps_data['i'].values - ps_data['z'].values
        convert = ps_data['i'].values - 0.3407 * color + 0.0026
        convert[(ps_data['i'].values<0)|(ps_data['z'].values<0)|(color<=-0.2)|(color>0.3)] = -999.
        return convert
    elif band=='z':
        color = ps_data['i'].values - ps_data['z'].values
        convert = ps_data['z'].values - 0.2575 * color - 0.0074
        convert[(ps_data['i'].values<0)|(ps_data['z'].values<0)|(color<=-0.2)|(color>0.3)] = -999.
        return convert
    elif band=='y':
        color = ps_data['i'].values - ps_data['z'].values
        convert = ps_data['z'].values - 0.6032 * color + 0.0185
        convert[(ps_data['i'].values<0)|(ps_data['z'].values<0)|(color<=-0.2)|(color>0.3)] = -999.
        return convert
    else:
        print 'Band not valid'

################################################################################

def doSet(args, data):
    # Delete previous *Obj.csv files
    oldfiles = glob.glob("*Obj.csv")
    for f in oldfiles:
        if os.path.isfile(f):
            os.remove(f)
        else:
            print("No old object files to be deleted")
    
    for i in range(len(data)):
        fullcat2Obj(args, data['FILENAME'][i], data['BAND'][i])
        
        match_outfile = "%s_match.csv" % (data['FILENAME'][i])
        obj_infile = "%s_Obj.csv" % (data['FILENAME'][i])
        std_infile = "%s_std.csv" % (data['FILENAME'][i])
        
        matchStars(std_infile, obj_infile, match_outfile, match_tolerance=1.0)
    return


def fullcat2Obj(args, filename, band):
    # Read SEX_table filename_fullcat.fits then select subsame and write it as filename_fullcat.fits_Obj.csv
    outfile = "%s_Obj.csv" % (filename)
    
    mag_type = args.magType.upper()
    flux_type = mag_type.replace('MAG', 'FLUX')
    flux_err_type = mag_type.replace('MAG', 'FLUXERR') 

    sex_cols = ['NUMBER', 'ALPHAWIN_J2000', 'DELTAWIN_J2000', flux_type, flux_err_type, 'SPREAD_MODEL', 'SPREADERR_MODEL', 'CLASS_STAR', 'FLAGS']
    SEXdata = fitsio.read(filename, columns=sex_cols, ext=2)[:]
    
    mask = (SEXdata[flux_type] > 1000) & (SEXdata['FLAGS'] <= 3) & (SEXdata['CLASS_STAR'] > 0.8) & (SEXdata['SPREAD_MODEL'] < 0.01)
    SEXdata = SEXdata[mask]
    SEXdata = SEXdata[np.argsort(SEXdata['ALPHAWIN_J2000'])]
    
    mag = -2.5 * np.log10(SEXdata[flux_type]) + args.sex_mag_zeropoint
    magerr = (2.5 / np.log(10.)) * (SEXdata[flux_err_type] / SEXdata[flux_type])
    
    outdata = np.array([SEXdata['NUMBER'],
                        SEXdata['ALPHAWIN_J2000'],
                        SEXdata['DELTAWIN_J2000'],
                        mag,
                        magerr,
                        np.repeat(args.sex_mag_zeropoint, SEXdata.size),
                        np.repeat(mag_type, SEXdata.size),
                        np.repeat(band, SEXdata.size)]).T
    
    outdata = pd.DataFrame(outdata, columns=['OBJECT_NUMBER','RA','DEC','MAG','MAGERR','ZEROPOINT','MAGTYPE','BAND'])
    outdata.to_csv(outfile, index=False)
    return


def matchStars(std_infile, obs_infile, match_outfile, match_tolerance=1.):
    '''
    Matches observed and standard stars
        match_tolerance: Maximum distance between matches in arcseconds
    
    Calculate the indices of objects in STD that correspond to the order of objects in OBS
    Identical to:
    for object in OBS:
        loop through STD and find STD object with least separation with this object
        find the STD index of that object
        put this index into an array, idx
    '''
    std_data = pd.read_csv(std_infile)
    obs_data = pd.read_csv(obs_infile)
    obs_coords = np.array([obs_data['RA'], obs_data['DEC']]).T
    std_coords = np.array([std_data['RA'], std_data['DEC']]).T
    
    coord_broadcast0 = np.repeat(std_coords[np.newaxis, :, :], len(obs_coords), axis=0)
    coord_broadcast1 = np.repeat(obs_coords[:, np.newaxis, :], len(std_coords), axis=1)
    
    separation_broadcast = vectorized_angsep(coord_broadcast0, coord_broadcast1)
    
    min_sep = np.degrees(np.min(separation_broadcast, axis=1))
    idx = np.argmin(separation_broadcast, axis=1)
    
    tolerance_mask = (min_sep < (match_tolerance/3600.)) # Matches should be within some tolerance criteria
    idx = idx[tolerance_mask]
    
    # Standard star columns are denoted with "_1" and Observed star columns are denoted with "_2"
    outdata = pd.merge(std_data.iloc[idx].reset_index(drop=True).add_suffix('_1'),
                       obs_data[tolerance_mask].reset_index(drop=True).add_suffix('_2'),
                       right_index=True,
                       left_index=True)
    outdata.insert(0, 'MATCHID', np.arange(len(outdata))+1)
    outdata.to_csv(match_outfile, index=False)
    return


def vectorized_angsep(coords1, coords2):
    '''
    Calculates angular separation using a vectorized Vincenty formula and outputs in a specific way
    Purpose-built for this function by Isaac M
    
    Based on astropy.coordinates.angular_separation
    '''
    rad_coords1, rad_coords2 = np.radians(coords1), np.radians(coords2)
    sdlon = np.sin(rad_coords2[:,:,0] - rad_coords1[:,:,0])
    cdlon = np.cos(rad_coords2[:,:,0] - rad_coords1[:,:,0])
    slat1 = np.sin(rad_coords1[:,:,1])
    slat2 = np.sin(rad_coords2[:,:,1])
    clat1 = np.cos(rad_coords1[:,:,1])
    clat2 = np.cos(rad_coords2[:,:,1])
    
    num1 = clat2 * sdlon
    num2 = clat1 * slat2 - slat1 * clat2 * cdlon
    denominator = slat1 * slat2 + clat1 * clat2 * cdlon
    return np.arctan2(np.hypot(num1, num2), denominator)

################################################################################

def sigmaClipZP_perCCD(args, data):
    zp_outfile = "Zero_D%08d_%02d_r%sp%1d.csv" % (args.expnum, args.ccd, args.reqnum, args.attnum)
    merged_outfile = "Merged_D%08d_%02d_r%sp%1d.csv" % (args.expnum, args.ccd, args.reqnum, args.attnum)

    lines = [[]]*len(data)
    for i in range(len(data)):
        match_file = "%s_match.csv" % (data['FILENAME'][i])

        try:
            match_data = pd.read_csv(match_file)
            
            mag_diff = match_data['MAG_2'].values - match_data['WAVG_MAG_PSF_1'].values - match_data['ZEROPOINT_2'].values
            mag_diff = mag_diff[(mag_diff < -10) & (mag_diff > -40)] # Cuts to ensure usability
            
            n_stars = len(mag_diff)
            mag_diff = sigma_clip(mag_diff, sigma=3, cenfunc=np.mean).compressed()
            n_after_clip = len(mag_diff)

            if n_after_clip > 2:
                sig_clip_zp = np.mean(mag_diff)
                std_sig_clip_zp = np.std(mag_diff) / np.sqrt(n_after_clip)
            else:
                sig_clip_zp = -999
                std_sig_clip_zp = -999
        except:
            sig_clip_zp = -999
            std_sig_clip_zp = -999
            n_after_clip = 0
            n_stars = 0

        lines[i] = (data['FILENAME'][i], n_stars, n_after_clip, sig_clip_zp, std_sig_clip_zp, args.magType) 
    
    out_cols = ['FILENAME', 'Nall', 'Nclipped', 'ZP', 'ZPrms', 'magType']
    
    zp_data = pd.DataFrame(lines, columns=out_cols)
    zp_data.to_csv(zp_outfile, index=False)
    
    merge_data = pd.merge(data, zp_data)
    merge_data.to_csv(merged_outfile, index=False)
    return


def sigmaClipZP_allCCD(args):
    std_file = "STDD%08d_r%sp%1d_red_catlist.csv" % (args.expnum, args.reqnum, args.attnum)
    obj_file = "ObjD%08d_r%sp%1d_red_catlist.csv" % (args.expnum, args.reqnum, args.attnum)
    match_file = "OUTD%08d_r%sp%1d_red_catlist.csv" % (args.expnum, args.reqnum, args.attnum)
    all_zp_outfile = "allZP_D%08d_r%sp%1d.csv" % (args.expnum, args.reqnum, args.attnum)
    
    #std_data = pd.read_csv(std_file).sort_values(['RA'], ascending=True)
    std_data = pd.read_csv(std_file).sort(['RA'], ascending=True)
    std_data.to_csv(std_file, index=False)
    
    # Read all Obj files and rewrite to a single file
    all_files = glob.glob("*Obj.csv")
    obj_data = pd.concat((pd.read_csv(f) for f in all_files))
    #obj_data = obj_data.sort_values(['RA'], ascending=True)
    obj_data = obj_data.sort(['RA'], ascending=True)
    obj_data.to_csv(obj_file, index=False)

    matchStars(std_file, obj_file, match_file, match_tolerance=1.0)

    try:
        match_data = pd.read_csv(match_file)
        
        mag_diff = match_data['MAG_2'].values - match_data['WAVG_MAG_PSF_1'].values - match_data['ZEROPOINT_2'].values
        mag_diff = mag_diff[(mag_diff < -10) & (mag_diff > -40)] # Cuts to ensure usability
        
        n_stars = len(mag_diff)
        mag_diff = sigma_clip(mag_diff, sigma=3, cenfunc=np.mean).compressed()
        n_after_clip = len(mag_diff)
        
        if n_after_clip > 2:
            sig_clip_zp = np.mean(mag_diff)
            std_sig_clip_zp = np.std(mag_diff) / np.sqrt(n_after_clip)
        else:
            sig_clip_zp = -999
            std_sig_clip_zp = -999
    except:
        sig_clip_zp = -999
        std_sig_clip_zp = -999
        n_after_clip = 0
        n_stars = 0
    
    outdata = pd.DataFrame(columns=['EXPNUM', 'REQNUM', 'ATTNUM', 'NumStarsAll', 'NumStarsClipped', 'sigclipZP', 'stdsigclipzp'])
    outdata.loc[0] = (args.expnum, args.reqnum, args.attnum, n_stars, n_after_clip, sig_clip_zp, std_sig_clip_zp)
    outdata.to_csv(all_zp_outfile, index=False)
    return


def findZP_outliers(args):
    # Searches for outliers and applies flags
    merged_file = "Merged_D%08d_%02d_r%sp%1d.csv" % (args.expnum, args.ccd, args.reqnum, args.attnum)
    all_zp_file = "allZP_D%08d_r%sp%1d.csv" % (args.expnum, args.reqnum, args.attnum)
    outfile = "Merg_allZP_D%08d_%02d_r%sp%1d.csv" % (args.expnum, args.ccd, args.reqnum, args.attnum)
    
    merged_data = pd.read_csv(merged_file)
    all_zp_data = pd.read_csv(all_zp_file)
    
    # These masks select for bad data
    outlier_mask = (merged_data['Nclipped'] < 4) | (merged_data['ZP'] < -100) | (merged_data['ZPrms'] > 0.3)
    merged_data['NewZP'] = np.where(outlier_mask, all_zp_data['sigclipZP'], merged_data['ZP'])
    merged_data['NewZPrms'] = np.where(outlier_mask, all_zp_data['stdsigclipzp'], merged_data['ZPrms'])
    merged_data['NewZPFlag1'] = np.where(outlier_mask, np.int16(1), np.int16(0))
    merged_data['DiffZP'] = merged_data['NewZP'] - np.median(merged_data['NewZP'])
    
    diff_mask = (abs(merged_data['DiffZP']) < 0.15)
    merged_data['NewZPFlag2'] = np.where(diff_mask, np.int16(0), np.int16(-1000))        
    merged_data['Percent1'] = 100.0 * np.count_nonzero(merged_data['NewZPFlag2']) / len(merged_data['NewZP'])
    
    percent_mask = (merged_data['Percent1'] >= 20) # If 20% of CCDs (i.e 12 CCDs out 60)
    merged_data['NewZPFlag3'] = np.where(percent_mask, np.int16(-9999), np.int16(0))
    
    merged_data['NewZPFlag'] = merged_data['NewZPFlag1'] + merged_data['NewZPFlag2'] + merged_data['NewZPFlag3']
    merged_data['DiffZP1'] = 1000.0 * merged_data['DiffZP']                                            
    
    merged_data.to_csv(merged_file, index=False) # Overwrite Merged File
    out_cols = ['FILENAME', 'EXPNUM', 'CCDNUM', 'NewZP', 'NewZPrms', 'NewZPFlag']
    merged_data.to_csv(outfile, columns=out_cols, index=False)
    return

################################################################################

def Onefile(args):
    merged_file = "Merg_allZP_D%08d_%02d_r%sp%1d.csv" % (args.expnum, args.ccd, args.reqnum, args.attnum)
    csv_outfile = "D%08d_%02d_r%sp%1d_ZP.csv" % (args.expnum, args.ccd, args.reqnum, args.attnum)
    fits_outfile = "D%08d_%02d_r%sp%1d_ZP.fits" % (args.expnum, args.ccd, args.reqnum, args.attnum)
    
    data = pd.read_csv(merged_file)
    for i in range(len(data)):
        applyZP2Obj(*data.iloc[i])
    
    obj_files = glob.glob("*Obj.csv")
    #data = pd.concat((pd.read_csv(f) for f in obj_files)).sort_values(['ALPHAWIN_J2000'], ascending=True)
    data = pd.concat((pd.read_csv(f) for f in obj_files)).sort(['ALPHAWIN_J2000'], ascending=True)
    data.insert(0, 'ID', np.arange(len(data)) + 1)
    
    data.to_csv(csv_outfile, index=False)

    # Later Please ADD new args for args.fits/args.csv with if one/or and
    # Currently BOTH csv and fits are written to disk with NO ARGS!
    fits_col_list = [[]]*len(data.columns)
    formats = ('J', 'I', 'I', 'I', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'D', 'I', 'I', 'D', 'D', 'I')
    for i in range(len(data.columns)):
        fits_col_list[i] = fits.Column(name=data.columns[i], format=formats[i], array=data[data.columns[i]].values)
    fits_cols = fits.ColDefs(fits_col_list)
    hdu = fits.BinTableHDU.from_columns(fits_cols)
    if os.path.isfile(fits_outfile):
        os.remove(fits_outfile)
    hdu.writeto(fits_outfile)
    return


def applyZP2Obj(catfile, EXPNUM, CCDNUM, zeropoint, zeropoint_rms, zeropoint_flag):
    # Read fullcat.fits then apply_ZP with FLAGS and write ONE file for all CCDs as fullcat.fits_Obj.csv
    outfile = "%s_Obj.csv" % (catfile)
    
    hdr = ['NUMBER', 'ALPHAWIN_J2000', 'DELTAWIN_J2000',
           'FLUX_AUTO', 'FLUXERR_AUTO', 'FLUX_PSF', 'FLUXERR_PSF',
           'MAG_AUTO', 'MAGERR_AUTO', 'MAG_PSF', 'MAGERR_PSF',
           'SPREAD_MODEL', 'SPREADERR_MODEL',
           'FWHM_WORLD', 'FWHMPSF_IMAGE', 'FWHMPSF_WORLD',
           'CLASS_STAR', 'FLAGS', 'IMAFLAGS_ISO']
    
    data = fitsio.read(catfile, columns=hdr, ext=2)[:]
    data = data[np.argsort(data['ALPHAWIN_J2000'])]
    
    #with np.testing.suppress_warnings() as suppress:
    #    suppress.filter(RuntimeWarning) # Warnings for invalid logarithms are expected and get filtered by np.where
    
    # Recalculate MAG_AUTO using new ZPs
    flux_mask = (data['FLUX_AUTO'] > 0.)
    data['MAG_AUTO'] = np.where(flux_mask, (-2.5*np.log10(data['FLUX_AUTO']) - zeropoint), np.int16(-9999))
    data['MAGERR_AUTO'] = np.where(flux_mask, (2.5/np.log(10.)) * (data['FLUXERR_AUTO'] / data['FLUX_AUTO']), np.int16(-9999))
    # Recalculate MAG_PSF using new ZPs
    flux_mask = (data['FLUX_PSF'] > 0.)
    data['MAG_PSF'] = np.where(flux_mask, (-2.5*np.log10(data['FLUX_PSF']) - zeropoint), np.int16(-9999)) 
    data['MAGERR_PSF'] = np.where(flux_mask, (2.5/np.log(10.)) * (data['FLUXERR_PSF'] / data['FLUX_PSF']), np.int16(-9999))
    
    lines = [[]]*len(data)
    for i in range(len(data)):
        lines[i] = (EXPNUM, CCDNUM, data['NUMBER'][i], data['ALPHAWIN_J2000'][i], data['DELTAWIN_J2000'][i],
                    data['FLUX_AUTO'][i], data['FLUXERR_AUTO'][i], data['FLUX_PSF'][i], data['FLUXERR_PSF'][i],
                    data['MAG_AUTO'][i], data['MAGERR_AUTO'][i], data['MAG_PSF'][i], data['MAGERR_PSF'][i],
                    data['SPREAD_MODEL'][i], data['SPREADERR_MODEL'][i],
                    data['FWHM_WORLD'][i], data['FWHMPSF_IMAGE'][i], data['FWHMPSF_WORLD'][i],
                    data['CLASS_STAR'][i], data['FLAGS'][i], data['IMAFLAGS_ISO'][i],
                    zeropoint, zeropoint_rms, zeropoint_flag)
    
    out_cols = ['EXPNUM', 'CCDNUM'] + hdr + ['ZeroPoint', 'ZeroPoint_rms', 'ZeroPoint_FLAGS']
    outdata = pd.DataFrame(lines, columns=out_cols)
    outdata.to_csv(outfile, index=False)
    return

################################################################################

def plotStandardAndObservedPositions(args, data):
    for i in range(len(data)):
        fullcat_file = data['FILENAME'][i]
        png_out = "%s.png" % (fullcat_file)
        obj_file = "%s_Obj.csv" % (fullcat_file)
        std_file = "%s_std.csv" % (fullcat_file)
        
        ccd_points = [[data['RAC2'][i], data['DECC2'][i]],
                      [data['RA_CENT'][i], data['DECC2'][i]],
                      [data['RAC3'][i], data['DECC3'][i]],
                      [data['RAC3'][i], data['DEC_CENT'][i]],
                      [data['RAC4'][i], data['DECC4'][i]],
                      [data['RA_CENT'][i], data['DECC4'][i]],
                      [data['RAC1'][i], data['DECC1'][i]],
                      [data['RAC1'][i], data['DEC_CENT'][i]]]
        ccd_line = plt.Polygon(ccd_points, fill=None, edgecolor='g')
        
        # Read in the file...
        std_data = pd.read_csv(std_file)
        obj_data = pd_read_csv(obj_file)
        plt.axes()
        plt.gca().add_patch(ccd_line)
        plt.scatter(std_data['RA'], std_data['DEC'], marker='.')
        plt.scatter(obj_data['RA'], obj_data['DEC'], c='r', marker='+')
        line = plt.Polygon(ccd_points, fill=None, edgecolor='r')

        plt.title(fullcat_file, color='#afeeee') 
        plt.savefig(png_out, format="png")
        plt.clf()
    return


def plotradec_ZP(args):
    catlist_file = "D%08d_r%sp%1d_red_catlist.csv" % (args.expnum, args.reqnum, args.attnum)
    merged_file = "Merged_D%08d_%02d_r%sp%1d.csv" % (args.expnum, args.ccd, args.reqnum, args.attnum)   
    png_out0 = "%s_ZP.png" % (catlist_file)
    png_out1 = "%s_deltaZP.png" % (catlist_file)
    png_out2 = "%s_NumClipstar.png" % (catlist_file)
    png_out3 = "%s_CCDsvsZPs.png" % (catlist_file)
    png_out4 = "%s_NewZP.png" % (catlist_file)
    png_out5 = "%s_NewdeltaZP.png" % (catlist_file)
    
    data = pd.read_csv(merged_file)
    
    if np.std(data['RA_CENT']) > 20:
        data['RA_CENT'] = roundRA(data['RA_CENT'])
        data['RAC1'] = roundRA(data['RAC1'])
        data['RAC2'] = roundRA(data['RAC2'])
        data['RAC3'] = roundRA(data['RAC3'])
        data['RAC4'] = roundRA(data['RAC4'])
    
    bad_mask = (data['ZP'] == -999)
    good_mask = (data['ZP'] > -999)
    bad_data = data[bad_mask]
    good_data = data[good_mask]
    
    if len(good_data)==0:
        print ("No Good Data to Plot! Exiting.")
        sys.exit(1)
    
    band = good_data['BAND'][0]
    zp_median = np.median(good_data['ZP'])
    
    no_flag_mask = (data['NewZPFlag'] == 0)
    hi_flag_mask = (data['NewZPFlag'] == 1)
    no_flag_data = data[no_flag_mask]
    hi_flag_data = data[hi_flag_mask]    
    
    minra = np.min(data[['RA_CENT', 'RAC1', 'RAC2', 'RAC3', 'RAC4']].values) - .075
    mindec = np.min(data[['DEC_CENT', 'DECC1', 'DECC2', 'DECC3', 'DECC4']].values) - .075
    maxra = np.max(data[['RA_CENT', 'RAC1', 'RAC2', 'RAC3', 'RAC4']].values) + .075
    maxdec = np.max(data[['DEC_CENT', 'DECC1', 'DECC2', 'DECC3', 'DECC4']].values) + .075
    
    # Plot the RA, DEC vs the expCal Zero-point mag
    l1 = plt.scatter(bad_data['RA_CENT'], bad_data['DEC_CENT'], c=bad_data['ZP'], s=15, marker=(25,0), cmap=mpl.cm.spectral, vmin=np.min(good_data['ZP']), vmax=np.max(good_data['ZP']))
    l2 = plt.scatter(good_data['RA_CENT'], good_data['DEC_CENT'], c=good_data['ZP'], s=500, marker=(5,0), cmap=mpl.cm.spectral, vmin=np.min(good_data['ZP']), vmax=np.max(good_data['ZP']))
    cbar = plt.colorbar(ticks=np.linspace(np.min(good_data['ZP']), np.max(good_data['ZP']), 4))    
    cbar.set_label('Zero-Point Mag')
    plt.legend((l1, l2), ('No Data','ExpCal'), scatterpoints=1, loc='upper left', ncol=1, fontsize=9)
    
    for i in range(len(data)):
        ccd_points = [[data['RAC2'][i], data['DECC2'][i]],
                      [data['RAC3'][i], data['DECC3'][i]],
                      [data['RAC4'][i], data['DECC4'][i]],
                      [data['RAC1'][i], data['DECC1'][i]]]
        ccd_line = plt.Polygon(ccd_points, fill=None, edgecolor='k')
        plt.gca().add_patch(ccd_line)
    
    plt.title('D%08d_r%sp%1d %s-Band ZP_Median=%.3f ' % (args.expnum, args.reqnum, args.attnum, band, zp_median))   
    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(png_out0, format="png")
    plt.clf() 
    
    # Plot the RA, DEC vs the expCal Delta Zero-point mag from median
    l1 = plt.scatter(bad_data['RA_CENT'], bad_data['DEC_CENT'], c=bad_data['ZP'], s=15,
                     marker=(25,0), cmap=mpl.cm.spectral, vmin=np.min(good_data['ZP']), vmax=np.max(good_data['ZP']))
    l2 = plt.scatter(good_data['RA_CENT'], good_data['DEC_CENT'], c=1000*(good_data['ZP']-zp_median), s=500,
                     marker=(5,0), cmap=mpl.cm.spectral, vmin=np.min(1000*(good_data['ZP'].values-zp_median)), vmax=np.max(1000*(good_data['ZP'].values-zp_median)))
    cbar = plt.colorbar(ticks=np.linspace(np.min(1000*(good_data['ZP'].values-zp_median)), np.max(1000*(good_data['ZP'].values-zp_median)), 4))    
    cbar.set_label('Delta Zero-Point mili-Mag')
    plt.legend((l1,l2), ('No Data','ExpCal'), scatterpoints=1, loc='upper left', ncol=1, fontsize=9)
    
    for i in range(len(data)):
        ccd_points = [[data['RAC2'][i], data['DECC2'][i]],
                      [data['RAC3'][i], data['DECC3'][i]],
                      [data['RAC4'][i], data['DECC4'][i]],
                      [data['RAC1'][i], data['DECC1'][i]]]
        ccd_line = plt.Polygon(ccd_points, fill=None, edgecolor='k')
        plt.gca().add_patch(ccd_line)
    
    plt.title('D%08d_r%sp%1d %s-Band ZP_Median=%.3f ' % (args.expnum, args.reqnum, args.attnum, band, zp_median))   
    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(png_out1, format="png")
    plt.clf() 
    
    # Plot RA DEC vs Number of stars clipped stars from expCal
    l1 = plt.scatter(bad_data['RA_CENT'], bad_data['DEC_CENT'], c=bad_data['Nclipped'], s=15, marker=(25,0), cmap=mpl.cm.spectral)
    l2 = plt.scatter(good_data['RA_CENT'], good_data['DEC_CENT'], c=good_data['Nclipped'], s=500, marker=(5,0), cmap=mpl.cm.spectral)
    cbar = plt.colorbar()
    cbar.set_label('No. 3 $\sigma$ clipped Stars')
    plt.legend((l1,l2), ('No Data','expCal'), scatterpoints=1, loc='upper left', ncol=1, fontsize=9)
    
    for i in range(len(data)):
        ccd_points = [[data['RAC2'][i], data['DECC2'][i]],
                      [data['RAC3'][i], data['DECC3'][i]],
                      [data['RAC4'][i], data['DECC4'][i]],
                      [data['RAC1'][i], data['DECC1'][i]]]
        ccd_line = plt.Polygon(ccd_points, fill=None, edgecolor='k')
        plt.gca().add_patch(ccd_line)
    
    plt.title('D%08d_r%sp%1d %s-Band ZP_Median=%.3f ' % (args.expnum, args.reqnum, args.attnum, band, zp_median))
    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(png_out2, format="png")
    plt.clf() 
    
    # Plot CCDs vs ZP from expCal
    plt.errorbar(bad_data['CCDNUM'], bad_data['ZP'], bad_data['ZPrms'], fmt='o', label='No Data')
    plt.errorbar(good_data['CCDNUM'], good_data['ZP'], good_data['ZPrms'], fmt='o',label='expCal')
    legend = plt.legend(loc='upper center', shadow=None, fontsize=12)
    legend.get_frame().set_facecolor('#00FFCC')
    plt.title('D%08d_r%sp%1d %s-Band ZP_Median=%.3f ' % (args.expnum, args.reqnum, args.attnum, band, zp_median))
    plt.xlabel(r"$CCDs$", size=18)
    plt.ylabel(r"$Zero Points$", size=18)
    plt.ylim(np.min(good_data['ZP'])-.01, np.max(good_data['ZP'])+.02)
    plt.xlim(np.min(good_data['CCDNUM'])-1.5, np.max(good_data['CCDNUM'])+1.5)
    plt.savefig(png_out3, format="png")
    plt.clf()
    
    # Plot the RA, DEC vs the NEW Zero-point mag
    l1 = plt.scatter(no_flag_data['RA_CENT'], no_flag_data['DEC_CENT'], c=no_flag_data['NewZP'], s=500,
                     marker=(5,0), cmap=mpl.cm.spectral, vmin=np.min(no_flag_data['NewZP']), vmax=np.max(no_flag_data['NewZP']))
    l2 = plt.scatter(hi_flag_data['RA_CENT'], hi_flag_data['DEC_CENT'], c=hi_flag_data['NewZP'], s=25,
                     marker=(25,0), cmap=mpl.cm.spectral, vmin=np.min(no_flag_data['NewZP']), vmax=np.max(no_flag_data['NewZP']))
    cbar = plt.colorbar(ticks=np.linspace(np.min(no_flag_data['NewZP']), np.max(no_flag_data['NewZP']), 4))    
    cbar.set_label('Zero-Point Mag')
    plt.legend((l1,l2), ('CCD','allEXP'), scatterpoints=1, loc='upper left', ncol=1, fontsize=9)
    
    for i in range(len(data)):
        ccd_points = [[data['RAC2'][i], data['DECC2'][i]],
                      [data['RAC3'][i], data['DECC3'][i]],
                      [data['RAC4'][i], data['DECC4'][i]],
                      [data['RAC1'][i], data['DECC1'][i]]]
        ccd_line = plt.Polygon(ccd_points, fill=None, edgecolor='k')
        plt.gca().add_patch(ccd_line)
    
    plt.title('D%08d_r%sp%1d %s-Band ' % (args.expnum, args.reqnum, args.attnum, band))   
    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(png_out4, format="png")
    plt.clf() 
    
    # Plot the RA, DEC vs the NEW Delta Zero-point mag from median
    l1 = plt.scatter(no_flag_data['RA_CENT'], no_flag_data['DEC_CENT'], c=no_flag_data['DiffZP1'], s=500,
                     marker=(5,0), cmap=mpl.cm.spectral, vmin=np.min(no_flag_data['DiffZP1']), vmax=np.max(no_flag_data['DiffZP1']))   
    l2 = plt.scatter(hi_flag_data['RA_CENT'], hi_flag_data['DEC_CENT'], c=hi_flag_data['DiffZP1'], s=25,
                     marker=(25,0), cmap=mpl.cm.spectral, vmin=np.min(no_flag_data['DiffZP1']), vmax=np.max(no_flag_data['DiffZP1']))
    cbar = plt.colorbar(ticks=np.linspace(np.min(no_flag_data['DiffZP1']), np.max(no_flag_data['DiffZP1']), 4))    
    cbar.set_label('Delta Zero-Point mili-Mag')
    plt.legend((l1,l2), ('CDD','allExP'), scatterpoints=1, loc='upper left', ncol=1, fontsize=9)
    
    for i in range(len(data)):
        ccd_points = [[data['RAC2'][i], data['DECC2'][i]],
                      [data['RAC3'][i], data['DECC3'][i]],
                      [data['RAC4'][i], data['DECC4'][i]],
                      [data['RAC1'][i], data['DECC1'][i]]]
        ccd_line = plt.Polygon(ccd_points, fill=None, edgecolor='k')
        plt.gca().add_patch(ccd_line)
    
    plt.title('D%08d_r%sp%1d %s-Band ' % (args.expnum, args.reqnum, args.attnum, band))
    plt.xlabel(r"$RA$", size=18)
    plt.ylabel(r"$DEC$", size=18)
    plt.xlim([minra,maxra])
    plt.ylim([mindec,maxdec])
    plt.savefig(png_out5, format="png")
    plt.clf()
    return


def roundRA(ra, cutoff=356):
    # I assume this is to avoid some singularity at 360 degrees
    # Cutoff changed to 350 on 10/09/17, but changed to 356 some time after
    return np.where((ra < cutoff), ra, ra - 360.)

################################################################################

if __name__ == "__main__":
    print ("Starting expCalib.py")
    if args.verbose > 0: print(args)
    
    print "Reading red_catlist..."
    catlist_path = "D%08d_r%sp%1d_red_catlist.csv" % (args.expnum, args.reqnum, args.attnum)
    red_catlist_data = pd.read_csv(catlist_path)
    red_catlist_data = red_catlist_data[red_catlist_data['CCDNUM'] == args.ccd] # Get only data for correct CCD
    print "Done!"
    
    print "Getting Standard Stars..."
    getallccdfromDELVE(args, red_catlist_data, catlist_path, remove_agns=True)
    print "Done!"
    
    print "Matching Stars..."
    doSet(args, red_catlist_data)
    print "Done!"

    # Plot locations of STD and OBS for each CCD
    if args.verbose > 0:
        print "Plotting Standard and Observed Stars..."
        plotStandardAndObservedPositions(args, red_catlist_data)
        print "Done!"
    
    print "Estimating Zero Point..."
    sigmaClipZP_perCCD(args, red_catlist_data)
    sigmaClipZP_allCCD(args)
    findZP_outliers(args)
    print "Done!"
    
    print "Writing to Output Files..."
    Onefile(args)
    print "Done!"

    # Plot ra,dec of matched stars for ALL CCDs
    # Comment this line for grid production
    #plotradec_ZP(args)
