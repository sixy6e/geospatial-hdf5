#!/usr/bin/env python

from affine import Affine
import h5py
from dtypes import NUMPY2KEADTYPE
from dtypes import KEA2NUMPYDTYPE
from dtypes import GDAL2KEADTYPE
from dtypes import KEA2GDALDTYPE

from rasterio.crs import from_string
from rasterio.crs import to_string
import osr
import collections
import numpy


IMAGE_VERSION = "1.2"
VERSION = "1.1"
FILETYPE = "KEA"
GENERATOR = "h5py"


def open(path, mode='r', width=None, height=None, count=None, transform=None,
         crs=None, no_data=None, dtype=None, chunks=(256, 256),
         blocksize=256, compression=1, band_names=None):

    # should we pass to different read write classes based on the mode?
    if mode == 'r':
        fid = h5py.File(path, mode)
        ds = KeaH5RDOnly(fid)
    elif mode == 'r+':
        fid = h5py.File(path, mode)
        ds = KeaH5RW(fid)
    elif mode =='w':
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
                         dtype, chunks, blocksize, compression, band_names)

        ds = KeaH5RW(fid)

    return ds
        

def create_kea_image(fid, width, height, count, transform, crs, no_data,
                     dtype, chunksize, blocksize, compression, band_names):
    """
    Initialises the KEA format layout
    """


    # group names for each band
    band_group_names = ['BAND{}'.format(i+1) for i in range(count)]

    # resolutio, ul corner tie point co-ordinate, rotation
    res = (transform[0], transform[4])
    ul = (transform[2], transform[5])
    rot = (transform[1], transform[3])

    # gdal or numpy number dtype value
    kea_dtype = NUMPY2KEADTYPE[dtype]

    # convert the proj4 dict to wkt
    sr = osr.SpatialReference()
    sr.ImportFromProj4(to_string(crs))
    crs_wkt = sr.ExportToWkt()

    # create band level groups
    band_groups = {}
    for gname in band_group_names:
        fid.create_group(gname)
        fid[gname].create_group('METADATA')

        # TODO need example data in order to flesh the overviews section
        fid[gname].create_group('OVERVIEWS')

        # dataset for our data and associated attributes
        fid[gname].create_dataset('DATA', shape=shape, dtype=dtype,
                                  compression=compression, chunks=chunks,
                                  fillvalue=no_data)

        # CLASS 'IMAGE', is a HDF recognised attribute
        fid[gname]['DATA'].attrs['CLASS'] = 'IMAGE'
        fid[gname]['DATA'].attrs['IMAGE_VERSION'] = IMAGE_VERSION
        fid[gname]['DATA'].attrs['BLOCK_SIZE'] = blocksize

        # KEA has defined their own numerical datatype mapping
        fid[gname].create_dataset('DATATYPE', shape=(1,),
                                  data=kea_dtype, dtype='uint16')

        # descriptors of the dataset
        # TODO what info can be populated here???
        fid[gname].create_dataset('DESCRIPTION', shape=(1,),
                                          data='')

        # we'll use a default, but all the user to overide later
        fid[gname].create_dataset('LAYER_TYPE', shape=(1,), data=0)
        fid[gname].create_dataset('LAYER_USAGE', shape=(1,), data=0)

        # TODO unclear on this section
        fid[gname].create_group('ATT/DATA')

        # TODO need an example in order to flesh the neighbours section
        fid[gname].create_group('ATT/NEIGHBOURS')

        # TODO unclear on header chunksize and size
        fid[gname].create_dataset('ATT/HEADER/CHUNKSIZE', data=0,
                                          dtype='uint64')
        fid[gname].create_dataset('ATT/HEADER/SIZE', data=[0,0,0,0,0],
                                          dtype='uint64')

        # do we have no a data value
        if no_data is not None:
            fid[gname].create_dataset('NO_DATA_VAL', shape=(1,),
                                      data=no_data)

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
        met.create_dataset(dname, shape=(1,), data=bname)

    # necessary image collection info
    hdr = fid.create_group('HEADER')

    # header datasets
    hdr.create_dataset('WKT', shape=(1,), data=crs_wkt)
    hdr.create_dataset('SIZE', data=(width, height), dtype='uint64')
    hdr.create_dataset('VERSION', shape=(1,), data=VERSION)
    hdr.create_dataset('RES', data=res, dtype='float64')
    hdr.create_dataset('TL', data=ul, dtype='float64')
    hdr.create_dataset('ROT', data=rot, dtype='float64')
    hdr.create_dataset('NUMBANDS', shape=(1,), data=count, dtype='uint16')
    hdr.create_dataset('FILETYPE', shape=(1,), data=FILETYPE)
    hdr.create_dataset('GENERATOR', shape=(1,), data=GENERATOR)

    # flush any cached items
    fid.flush()


# TODO
# maybe we could use a base class that retrieves and sets
# the various properties such as header, width, height, crs, transfrom etc

class KeaH5RDOnly(object):

    def __init__(self, fid):
        self._fid = fid
        self.header = self._read_header()
        self.width = self.header['SIZE'][0]
        self.height = self.header['SIZE'][1]
        self.count = self.header['NUMBANDS']
        self.band_groups = self._band_groups()
        self.band_datasets = self._band_datasets()

    def _read_header(self):
        _hdr = self._fid['HEADER']
        hdr = {}
        for key in _hdr:
            hdr[key] = _hdr[key][:]
        return hdr

    def _band_groups(self):
        gname_fmt = 'BAND{}'
        band_groups = {}
        for band in range(1, self.count + 1):
            grp = gname_fmt.format(band)
            band_groups[band] = self._fid[grp]
        return band_groups

    def _band_datasets(self):
        bname_fmt = 'BAND{}/DATA'
        bnd_dsets = {}
        for band in range(1, self.count + 1):
            dset = bname_fmt.format(band)
            bnd_dsets[band] = self._fid[dset]
        return bnd_dsets

    @property
    def crs(self):
        sr = osr.SpatialReference()
        sr.ImportFromWkt(self.header['WKT'][0])
        crs = from_string(sr.ExportToProj4())
        return crs

    @property
    def transform(self):
        transform = [self.header['TL'][0],
                     self.header['RES'][0],
                     self.header['ROT'][0],
                     self.header['TL'][1],
                     self.header['ROT'][1],
                     self.header['RES'][1]]
        return Affine.from_gdal(*transform)

    @property
    def no_data(self):
        # Do we return a no data value for each band dataset???
        # dict mapping or plain list. dict would allow different nan's
        # for different bands
        no_data = []
        for bgrp in self.band_groups:
            no_data.append(self.band_groups[bgrp]['NO_DATA_VAL'][0])
        return no_data

    @property
    def dtypes(self):
        dtypes = []
        for bgrp in self.band_groups:
            dtype = self.band_groups[bgrp]['DATATYPE'][0]
            dtypes.append(KEA2NUMPYDTYPE[dtype])
        return dtypes

    @property
    def chunks(self):
        chunks = self.band_datasets[1].chunks
        return chunks

    @property
    def metadata(self):
        metadata = {}
        for key in self._fid['METADATA']:
            metadata[key] = self._fid[key][:]

    @property
    def description(self):
        description = []
        for bgrp in self.band_groups:
            description.append(self.band_groups[bgrp]['DESCRIPTION'][0])
        return description

    # TODO have a dict mapping to a string equivalent
    @property
    def layer_useage(self):
        layer_useage = []
        for bgrp in self.band_groups:
            layer_useage.append(self.band_groups[bgrp]['LAYER_USAGE'][0])
        return layer_useage

    # TODO have a dict mapping to a string equivalent
    @property
    def layer_type(self):
        layer_type = []
        for bgrp in self.band_groups:
            layer_type.append(self.band_groups[bgrp]['LAYER_TYPE'][0])
        return layer_type

    # TODO retrieve the metadata and band names

    def read(self, bands, window=None):
        if isinstance(bands, collections.Sequence):
            nb = len(bands)
            if window is None:
                data = numpy.zeros((nb, self.height, self.width),
                                   dtype=self.dtypes[0])
                for i, band in enumerate(bands):
                    self.band_datasets[band].read_direct(data[i])
            else:
                ys, ye = window[0]
                xs, xe = window[1]
                ysize = ye - ys
                xsize = xe - xs
                idx = numpy.s_[ys:ye, xs:xe]
                data = numpy.zeros((nb, ysize, xsize),
                                   dtype=self.dtypes[0])
                for i, band in enumerate(bands):
                    self.band_datasets[band].read_direct(data[i], idx)
        else:
            if window is None:
                data = self.band_datasets[bands][:]
            else:
                ys, ye = window[0]
                xs, xe = window[1]
                idx = numpy.s_[ys:ye, xs:xe]
                data = self.band_datasets[bands][idx]
                
        return data


# This should probably be a subclass of ReadOnly, in order to avoid
# duplicating the read function
#class KeaH5RW(object):
#
#    def __init__(fid):
#    def read(bands, window):
#    def write(bands, window):
