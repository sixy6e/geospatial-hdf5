#!/usr/bin/env python

import collections
from affine import Affine
import h5py
from rasterio.crs import from_string
from rasterio.crs import to_string
import osr
import numpy

from geoh5.kea.dtypes import NUMPY2KEADTYPE
from geoh5.kea.dtypes import KEA2NUMPYDTYPE
from geoh5.kea.dtypes import GDAL2KEADTYPE
from geoh5.kea.dtypes import KEA2GDALDTYPE
from geoh5.kea.dtypes import fixed_length
from geoh5.kea._keaio import KeaImageRead, KeaImageReadWrite


IMAGE_VERSION = "1.2"
VERSION = "1.1"
FILETYPE = "KEA"
GENERATOR = "h5py"


def open(path, mode='r', width=None, height=None, count=None, transform=None,
         crs=None, no_data=None, dtype=None, chunks=(256, 256),
         blocksize=256, compression=1, band_names=None, fixed=False):

    # should we pass to different read write classes based on the mode?
    if mode == 'r':
        fid = h5py.File(path, mode)
        ds = KeaImageRead(fid)
    elif mode == 'r+':
        fid = h5py.File(path, mode)
        ds = KeaImageReadWrite(fid)
    elif mode == 'w':
        # Check we have all the necessary creation options
        if (width is None) or (height is None):
            msg = "Error. Both width and height must be specified."
            raise ValueError(msg)

        if dtype is None:
            msg = "Error. The dtype must be specifified."
            raise ValueError(msg)

        if count is None:
            msg = "Error. The count must be specified."
            raise ValueError(msg)

        # If we have no transform, default to image co-ordinates
        if (transform is None) or (crs is None):
            ul = (0, 0)
            rot = (0, 0)
            res = (1, -1)
            transform = Affine.from_gdal(*[0.0, 1.0, 0.0, 0.0, 0.0, -1.0])
            crs = ""

        if (chunks[0] > height) or (chunks[1] > width):
            msg = "The chunks must not exceed the width or height."
            raise ValueError(msg)

        # we'll use rasterio's proj4 dict mapping
        if not isinstance(crs, dict):
            msg = "Error. The crs is not a valid proj4 dict style mapping."
            raise ValueError(msg)

        # we'll follow rasterio in using an affine
        if not isinstance(transform, Affine):
            msg = "Error. The transform is not an Affine instance."
            transform = Affine.from_gdal(*transform)

        fid = h5py.File(path, mode)
        create_kea_image(fid, width, height, count, transform, crs, no_data,
                         dtype, chunks, blocksize, compression, band_names, fixed)

        ds = KeaImageReadWrite(fid)

    # populate the Kea class
    ds._read_kea()

    return ds


def create_kea_image(fid, width, height, count, transform, crs, no_data,
                     dtype, chunks, blocksize, compression, band_names,
                     fixed):
    """
    Initialises the KEA format layout
    """


    # group names for each band
    band_group_names = ['BAND{}'.format(i+1) for i in range(count)]

    # resolution, ul corner tie point co-ordinate, rotation
    res = (transform[0], transform[4])
    ul = (transform[2], transform[5])
    rot = (transform[1], transform[3])

    # gdal or numpy number dtype value
    kea_dtype = NUMPY2KEADTYPE[dtype]

    # convert the proj4 dict to wkt
    sr = osr.SpatialReference()
    sr.ImportFromProj4(to_string(crs))
    crs_wkt = sr.ExportToWkt()

    # image dimensions
    dims = (height, width)

    # create band level groups
    for gname in band_group_names:
        grp = fid.create_group(gname)
        md = grp.create_group('METADATA')

        # TODO need example data in order to flesh the overviews section
        oview = grp.create_group('OVERVIEWS')

        # dataset for our data and associated attributes
        dset = grp.create_dataset('DATA', shape=dims, dtype=dtype,
                                  compression=compression, chunks=chunks,
                                  fillvalue=no_data)

        # CLASS 'IMAGE', is a HDF recognised attribute
        if fixed:
            sid = h5py.h5s.create(h5py.h5s.SCALAR)
            tid = fixed_length('IMAGE')
            attr = h5py.h5a.create(dset.id, 'CLASS', tid, sid)
            attr.write(numpy.array('IMAGE'))
            sid = h5py.h5s.create(h5py.h5s.SCALAR)
            tid = fixed_length(IMAGE_VERSION)
            attr = h5py.h5a.create(dset.id, 'IMAGE_VERSION', tid, sid)
            attr.write(numpy.array(IMAGE_VERSION))
        else:
            dset.attrs['CLASS'] = 'IMAGE'
            dset.attrs['IMAGE_VERSION'] = IMAGE_VERSION

        # image blocksize
        dset.attrs['BLOCK_SIZE'] = blocksize

        # KEA has defined their own numerical datatype mapping
        fid[gname].create_dataset('DATATYPE', shape=(1,),
                                  data=kea_dtype, dtype='uint16')

        # descriptors of the dataset
        # TODO what info can be populated here???
        desc = ''
        if fixed:
            sid = h5py.h5s.create(h5py.h5s.SCALAR)
            tid = fixed_length(desc)
            dset = h5py.h5d.create(grp.id, 'DESCRIPTION', tid, sid)
            dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(desc))
        else:
            grp.create_dataset('DESCRIPTION', shape=(1,), data=desc)

        # we'll use a default, but all the user to overide later
        grp.create_dataset('LAYER_TYPE', shape=(1,), data=0)
        grp.create_dataset('LAYER_USAGE', shape=(1,), data=0)

        # TODO unclear on this section
        grp.create_group('ATT/DATA')

        # TODO need an example in order to flesh the neighbours section
        grp.create_group('ATT/NEIGHBOURS')

        # TODO unclear on header chunksize and size
        grp.create_dataset('ATT/HEADER/CHUNKSIZE', data=0, dtype='uint64')
        grp.create_dataset('ATT/HEADER/SIZE', data=[0,0,0,0,0], dtype='uint64')

        # do we have no a data value
        if no_data is not None:
            grp.create_dataset('NO_DATA_VAL', shape=(1,), data=no_data)

    # Some groups like GCPS will be empty depending on the type of image
    # being written to disk. As will some datasets.
    # TODO need an example in order to flesh out the GCP's section
    fid.create_group('GCPS')

    # TODO what else can we put in the metadata section
    met = fid.create_group('METADATA')

    # if we don't have an band names defined, create default values
    if band_names is None:
        band_names = ['Band {}'.format(bn + 1) for bn in range(count)]
    elif len(band_names) != count:
        # overwrite the user (probably should notify the user)
        bname_format = 'Band {}'
        band_names = ['Band {}'.format(bn + 1) for bn in range(count)]

    # write the band names to the METADATA group, as individually
    # named datasets of the form 'Band_n'; n=1..nbands
    dname_fmt = 'Band_{}'
    for i, bname in enumerate(band_names):
        dname = dname_fmt.format(i + 1)
        if fixed:
            sid = h5py.h5s.create(h5py.h5s.SCALAR)
            tid = fixed_length(bname)
            dset = h5py.h5d.create(met.id, dname, tid, sid)
            dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(bname))
        else:
            met.create_dataset(dname, shape=(1,), data=bname)

    # necessary image collection info
    hdr = fid.create_group('HEADER')

    # header datasets
    hdr.create_dataset('SIZE', data=dims, dtype='uint64')
    hdr.create_dataset('RES', data=res, dtype='float64')
    hdr.create_dataset('TL', data=ul, dtype='float64')
    hdr.create_dataset('ROT', data=rot, dtype='float64')
    hdr.create_dataset('NUMBANDS', shape=(1,), data=count, dtype='uint16')
    if fixed:
        wkt = bytes(crs_wkt)
        sid = h5py.h5s.create(h5py.h5s.SCALAR)
        tid = fixed_length(crs_wkt)
        dset = h5py.h5d.create(hdr.id, 'WKT', tid, sid)
        dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(wkt))

        sid = h5py.h5s.create(h5py.h5s.SCALAR)
        tid = fixed_length(VERSION)
        dset = h5py.h5d.create(hdr.id, 'VERSION', tid, sid)
        dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(VERSION))

        sid = h5py.h5s.create(h5py.h5s.SCALAR)
        tid = fixed_length(FILETYPE)
        dset = h5py.h5d.create(hdr.id, 'FILETYPE', tid, sid)
        dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(FILETYPE))

        sid = h5py.h5s.create(h5py.h5s.SCALAR)
        tid = fixed_length(GENERATOR)
        dset = h5py.h5d.create(hdr.id, 'GENERATOR', tid, sid)
        dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(GENERATOR))
    else:
        hdr.create_dataset('WKT', shape=(1,), data=crs_wkt)
        hdr.create_dataset('VERSION', shape=(1,), data=VERSION)
        hdr.create_dataset('FILETYPE', shape=(1,), data=FILETYPE)
        hdr.create_dataset('GENERATOR', shape=(1,), data=GENERATOR)

    # flush any cached items
    fid.flush()


# parallel hdf doesn't support variable length types...yet
# see https://www.hdfgroup.org/hdf5-quest.html#pvl
# non-parallel is fine
def create_kea_image_fixed(fid, width, height, count, transform, crs, no_data,
                           dtype, chunks, compression, blocksize, band_names):

    # dataset names for each band
    bnames = ['BAND{}'.format(i+1) for i in range(count)]

    # resolution, ul corner tie point co-ordinate, rotation
    res = (transform[0], transform[4])
    ul = (transform[2], transform[5])
    rot = (transform[1], transform[3])

    # gdal or numpy number dtype value
    kea_dtype = NUMPY2KEADTYPE[dtype]

    # convert the proj4 dict to wkt
    sr = osr.SpatialReference()
    sr.ImportFromProj4(to_string(crs))
    crs_wkt = sr.ExportToWkt()

    # image dimensions
    dims = (height, width)

    # create band level groups
    band_groups = {}
    for bname in bnames:
        band_groups[bname] = fid.create_group(bname)
        band_groups[bname].create_group('METADATA')
        band_groups[bname].create_group('OVERVIEWS')
        band_groups[bname].create_dataset('DATA', shape=dims, dtype=dtype,
                                          compression=compression,
                                          chunks=chunks, fillvalue=no_data)

        grp = band_groups[bname]['DATA']
        sid = h5py.h5s.create(h5py.h5s.SCALAR)
        tid = fixed('IMAGE')
        attr = h5py.h5a.create(grp.id, 'CLASS', tid, sid)
        attr.write(numpy.array('IMAGE'))

        sid = h5py.h5s.create(h5py.h5s.SCALAR)
        tid = fixed(IMAGE_VERSION)
        attr = h5py.h5a.create(grp.id, 'IMAGE_VERSION', tid, sid)
        attr.write(numpy.array(IMAGE_VERSION))

        grp.attrs['BLOCK_SIZE'] = blocksize

        band_groups[bname].create_dataset('DATATYPE', shape=(1,),
                                          data=kea_dtype, dtype='uint16')

        grp = band_groups[bname]
        sid = h5py.h5s.create(h5py.h5s.SCALAR)
        tid = fixed('')
        dset = h5py.h5d.create(grp.id, 'DESCRIPTION', tid, sid)
        dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(''))

        band_groups[bname].create_dataset('LAYER_TYPE', shape=(1,), data=0)
        band_groups[bname].create_dataset('LAYER_USAGE', shape=(1,), data=0)
        band_groups[bname].create_group('ATT/DATA')
        band_groups[bname].create_group('ATT/NEIGHBOURS')
        band_groups[bname].create_dataset('ATT/HEADER/CHUNKSIZE', data=0,
                                          dtype='uint64')
        band_groups[bname].create_dataset('ATT/HEADER/SIZE', data=[0,0,0,0,0],
                                          dtype='uint64')

        if no_data is not None:
            band_groups[bname].create_dataset('NO_DATA_VAL', shape=(1,),
                                              data=no_data)

    # Some groups like GCPS will be empty depending on the type of image
    # being written to disk. As will some datasets.
    fid.create_group('GCPS')
    met = fid.create_group('METADATA')
    hdr = fid.create_group('HEADER')


    if band_names is None:
        band_names = ['Band {}'.format(bn + 1) for bn in range(count)]
    elif len(band_names) != nbands:
        # overwrite the user (probably should notify the user)
        bname_format = 'Band {}'
        band_names = ['Band {}'.format(bn + 1) for bn in range(count)]


    # write the band names to the METADATA group, as individually
    # named datasets of the form 'Band_n'; n=1..nbands
    dname_fmt = 'Band_{}'
    for i, bname in enumerate(band_names):
        dname = dname_fmt.format(i + 1)
        sid = h5py.h5s.create(h5py.h5s.SCALAR)
        tid = fixed(dname)
        dset = h5py.h5d.create(met.id, dname, tid, sid)
        dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(bname))

    # header dsets
    wkt = bytes(crs_wkt)
    sid = h5py.h5s.create(h5py.h5s.SCALAR)
    tid = fixed(crs_wkt)
    dset = h5py.h5d.create(hdr.id, 'WKT', tid, sid)
    dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(wkt))

    hdr.create_dataset('SIZE', data=dims, dtype='uint64')

    sid = h5py.h5s.create(h5py.h5s.SCALAR)
    tid = fixed(VERSION)
    dset = h5py.h5d.create(hdr.id, 'VERSION', tid, sid)
    dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(VERSION))

    hdr.create_dataset('RES', data=res, dtype='float64')
    hdr.create_dataset('TL', data=ul, dtype='float64')
    hdr.create_dataset('ROT', data=rot, dtype='float64')
    hdr.create_dataset('NUMBANDS', shape=(1,), data=count, dtype='uint16')

    sid = h5py.h5s.create(h5py.h5s.SCALAR)
    tid = fixed(FILETYPE)
    dset = h5py.h5d.create(hdr.id, 'FILETYPE', tid, sid)
    dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(FILETYPE))

    sid = h5py.h5s.create(h5py.h5s.SCALAR)
    tid = fixed(GENERATOR)
    dset = h5py.h5d.create(hdr.id, 'GENERATOR', tid, sid)
    dset.write(h5py.h5s.ALL, h5py.h5s.ALL, numpy.array(GENERATOR))
