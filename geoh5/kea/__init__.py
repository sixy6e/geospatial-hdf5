#!/usr/bin/env python

import collections
from affine import Affine
import h5py
import numpy

_MPI = True
try:
    from mpi4py import MPI
except ImportError:
    _MPI = False
    _MSG = "MPI not available."

from geoh5.kea import common as kc
from geoh5.kea._keaio import KeaImageRead, KeaImageReadWrite


def open(path, mode='r', width=None, height=None, count=None, transform=None,
         crs=None, no_data=None, dtype=None,  blocksize=256,
         compression=1, band_names=None, parallel=False):
    """
    Opens a filelike object for a KEA image formatted HDF5 file.

    :param path:
        A string containing a full file path name to either create,
        modify or read a KEA image formatted HDF5 file.

    :param mode:
        A string containing the desired I/O mode.
        
        * `r`: Readonly.
        * `w`: Write. If file exists it will be overwritten.
        * `r+`: Read/Write.

    :param width:
        An integer representin the `x dimension` of the image dataset.
        Only used when `mode=w'.

    :param height:
        An integer representing the `y dimension` of the image dataset.
        Only used when `mode=w'.

    :param count:
        An integer representing the number of raster bands to be created.
        Only used when `mode=w'.

    :param transform:
        An `affine` representing the affine transformation of the image.
        Only used when `mode=w'.

    :param crs:
        A WKT string representation of a Co-ordinate Reference System.
        Only used when `mode=w'.

    :param no_data:
        An integer or floating point value representing the no data or
        fillvalue of the image datasets.
        Only used when `mode=w'.

    :param dtype:
        A string containing a `NumPy` datatype.
        Only used when `mode=w'.
        Valid types are:

        * int8
        * int16
        * int32
        * int64
        * uint8
        * uint16
        * uint32
        * uint64
        * float32
        * float64

    :param blocksize:
        An integer representing the desired blocksize.
        Defaults to 256.
        Only used when `mode=w'.

    :param compression:
        An integer in the range (0, 9), with 0 being low compression
        and 9 being high compression using the `gzip` filter.
        Default is 1. Will be set to `None` when `parallel` is set
        to True.
        Only used when `mode=w'.

    :param band_names:
        A list containing the raster band names for each raster
        band dataset.
        If `None`, then the raster band names will default to:

        * Band 1
        * Band 2
        * Band n

    :param parallel:
        A boolean indicating whether the file is to be written in
        parallel mode using `MPI`. `compression` will be set to
        `None` when `parallel` is set to `True`.
        Default is `False`.
        Only used when `mode=w'.

    :notes:
        Parallel HDF5 doesn't support variable length types...yet                      
        see https://www.hdfgroup.org/hdf5-quest.html#pvl
        
        Parallel HDF5 also doesn't support compression.
    """

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
            msg = "Both width and height must be specified."
            raise ValueError(msg)

        if dtype is None:
            msg = "The dtype must be specifified."
            raise ValueError(msg)

        if count is None:
            msg = "The count must be specified."
            raise ValueError(msg)

        # If we have no transform, default to image co-ordinates
        if (transform is None) or (crs is None):
            ul = (0, 0)
            rot = (0, 0)
            res = (1, -1)
            transform = Affine.from_gdal(*[0.0, 1.0, 0.0, 0.0, 0.0, -1.0])
            if parallel:
                crs = " "
            else:
                crs = ""

        if (blocksize > height) or (blocksize > width):
            msg = "The blocksize must not exceed the width or height."
            raise ValueError(msg)

        # we'll follow rasterio in using an affine
        if not isinstance(transform, Affine):
            msg = "The transform is not an Affine instance."
            transform = Affine.from_gdal(*transform)

        if parallel:
            if not _MPI:
                raise ImportError(_MSG)
            fid = h5py.File(path, mode, driver='mpio', comm=MPI.COMM_WORLD)
            compression = None
        else:
            fid = h5py.File(path, mode)
        create_kea_image(fid, width, height, count, transform, crs, no_data,
                         dtype, blocksize, compression, band_names,
                         parallel)

        ds = KeaImageReadWrite(fid)

    # populate the Kea class
    ds._read_kea()

    return ds


def create_kea_image(fid, width, height, count, transform, crs, no_data,
                     dtype, blocksize, compression, band_names, parallel):
    """
    Initialises the KEA image format layout.

    :param fid:
        The opened `h5py` dataset.  Referred to as a `file id`.

    :param width:
        An integer representin the `x dimension` of the image dataset.
        Only used when `mode=w'.

    :param height:
        An integer representing the `y dimension` of the image dataset.
        Only used when `mode=w'.

    :param count:
        An integer representing the number of raster bands to be created.
        Only used when `mode=w'.

    :param transform:
        An `affine` representing the affine transformation of the image.
        Only used when `mode=w'.

    :param crs:
        A WKT string representation of a Co-ordinate Reference System.
        Only used when `mode=w'.

    :param no_data:
        An integer or floating point value representing the no data or
        fillvalue of the image datasets.
        Only used when `mode=w'.

    :param dtype:
        A string containing a `NumPy` datatype.
        Only used when `mode=w'.
        Valid types are:

        * int8
        * int16
        * int32
        * int64
        * uint8
        * uint16
        * uint32
        * uint64
        * float32
        * float64

    :param blocksize:
        An integer representing the desired blocksize.
        Defaults to 256.
        Only used when `mode=w'.

    :param compression:
        An integer in the range (0, 9), with 0 being low compression
        and 9 being high compression using the `gzip` filter.
        Default is 1. Will be set to `None` when `parallel` is set
        to True.
        Only used when `mode=w'.

    :param band_names:
        A list containing the raster band names for each raster
        band dataset.
        If `None`, then the raster band names will default to:

        * Band 1
        * Band 2
        * Band n

    :param parallel:
        A boolean indicating whether the file is to be written in
        parallel mode using `MPI`. `compression` will be set to
        `None` when `parallel` is set to `True`.
        Default is `False`.
        Only used when `mode=w'.

    :notes:
        Parallel HDF5 doesn't support variable length types...yet                      
        see https://www.hdfgroup.org/hdf5-quest.html#pvl
        
        Parallel HDF5 also doesn't support compression.
    """

    # group names for each band
    band_group_names = ['BAND{}'.format(i+1) for i in range(count)]

    # resolution, ul corner tie point co-ordinate, rotation
    res = (transform[0], transform[4])
    ul = (transform[2], transform[5])
    rot = (transform[1], transform[3])

    # gdal or numpy number dtype value
    kea_dtype = kc.KeaDataType[dtype].value

    # image dimensions
    dims = (height, width)

    # define the square chunks
    chunks = (blocksize, blocksize)

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
        dset.attrs['CLASS'] = 'IMAGE'
        dset.attrs['IMAGE_VERSION'] = kc.IMAGE_VERSION

        # image blocksize
        dset.attrs['BLOCK_SIZE'] = blocksize

        # KEA has defined their own numerical datatype mapping
        fid[gname].create_dataset('DATATYPE', shape=(1,),
                                  data=kea_dtype, dtype='uint16')

        # descriptors of the dataset
        # TODO what info can be populated here???
        # if we are to re-write the description created using fixed length
        # we need to delete the existing dataset first
        desc = ''
        if parallel:
            grp.create_dataset('DESCRIPTION', shape=(1,),
                               data=numpy.string_(desc))
        else:
            grp.create_dataset('DESCRIPTION', shape=(1,), data=desc)

        # we'll use a default, but allow the user to overide later
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
        if parallel:
            stype = kc.STRFMT.format(length=len(bname))
            met.create_dataset(dname, shape=(1,), data=numpy.string_(bname),
                               dtype=stype)
        else:
            met.create_dataset(dname, shape=(1,), data=bname)

    # necessary image collection info
    hdr = fid.create_group('HEADER')

    # header datasets
    hdr.create_dataset('SIZE', data=dims[::-1], dtype='uint64')
    hdr.create_dataset('RES', data=res, dtype='float64')
    hdr.create_dataset('TL', data=ul, dtype='float64')
    hdr.create_dataset('ROT', data=rot, dtype='float64')
    hdr.create_dataset('NUMBANDS', shape=(1,), data=count, dtype='uint16')
    if parallel:
        wkt = numpy.string_(bytes(crs))
        stype = kc.STRFMT.format(length=len(wkt))
        hdr.create_dataset('WKT', shape=(1,), data=wkt, dtype=stype)

        stype = kc.STRFMT.format(length=len(kc.VERSION))
        hdr.create_dataset('VERSION', shape=(1,),
                           data=numpy.string_(kc.VERSION), dtype=stype)

        stype = kc.STRFMT.format(length=len(kc.FILETYPE))
        hdr.create_dataset('FILETYPE', shape=(1,),
                           data=numpy.string_(kc.FILETYPE), dtype=stype)

        stype = kc.STRFMT.format(length=len(kc.GENERATOR))
        hdr.create_dataset('GENERATOR', shape=(1,),
                           data=numpy.string_(kc.GENERATOR), dtype=stype)
    else:
        hdr.create_dataset('WKT', shape=(1,), data=crs)
        hdr.create_dataset('VERSION', shape=(1,), data=kc.VERSION)
        hdr.create_dataset('FILETYPE', shape=(1,), data=kc.FILETYPE)
        hdr.create_dataset('GENERATOR', shape=(1,), data=kc.GENERATOR)

    # flush any cached items
    fid.flush()
