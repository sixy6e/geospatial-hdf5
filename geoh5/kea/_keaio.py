#!/usr/bin/env python

from affine import Affine
import h5py

from rasterio.crs import from_string
from rasterio.crs import to_string
import osr
import collections
import numpy

from geoh5.kea.dtypes import NUMPY2KEADTYPE
from geoh5.kea.dtypes import KEA2NUMPYDTYPE
from geoh5.kea.dtypes import GDAL2KEADTYPE
from geoh5.kea.dtypes import KEA2GDALDTYPE


class KeaImageRead(object):
    
    def __init__(self, fid):
        self._fid = fid
        self._header = None
        self._closed = False
        
        # image dimensions
        self._width = None
        self._height = None
        self._count = None

        # spatial info
        self._crs_wkt = None
        self._crs = None
        self._transform = None

        self._band_groups = None
        self._band_datasets = None

        # band level info
        self._dtypes = None
        self._no_data = None
        self._chunks = None
        self._metadata = None
        self._description = None
        self._layer_useage = None
        self._layer_type = None

        # do we kick it off???
        # self._read_kea()

    def _read_kea(self):
        self._header = self._read_header()
        self._width = self._header['SIZE'][0]
        self._height = self._header['SIZE'][1]
        self._count = self._header['NUMBANDS'][0]
        self._crs_wkt = self._header['WKT'][0]
        self._crs = self._convert_wkt()
        self._transform = self._read_transform()
        self._band_groups = self._read_band_groups()
        self._band_datasets = self._read_band_datasets()
        self._dtypes = self._read_dtypes()
        self._dtype = self.dtypes[1] # should be all the same datatype
        self._no_data = self._read_no_data()
        self._chunks = self._read_chunks()
        self._metadata = self._read_metadata()
        self._description = self._read_description()
        self._layer_useage = self._read_layer_useage()
        self._layer_type = self._read_layer_type()

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def close(self):
        self._closed = True
        self._fid.close()

    def _read_header(self):
        _hdr = self._fid['HEADER']
        hdr = {}
        for key in _hdr:
            hdr[key] = _hdr[key][:]
        return hdr

    @property
    def closed(self):
        return self._closed

    @property
    def count(self):
        return self._count

    @property
    def width(self):
        return self._width

    @property
    def height(self):
        return self._height

    @property
    def crs_wkt(self):
        return self._crs_wkt

    @property
    def crs(self):
        return self._crs

    def _convert_wkt(self):
        sr = osr.SpatialReference()
        result = sr.ImportFromWkt(self.crs_wkt)
        crs = from_string(sr.ExportToProj4())
        return crs

    @property
    def transform(self):
        return self._transform

    def _read_transform(self):
        transform = [self._header['TL'][0],
                     self._header['RES'][0],
                     self._header['ROT'][0],
                     self._header['TL'][1],
                     self._header['ROT'][1],
                     self._header['RES'][1]]
        return Affine.from_gdal(*transform)

    def _read_band_groups(self):
        gname_fmt = 'BAND{}'
        band_groups = {}
        for band in range(1, self.count + 1):
            group = gname_fmt.format(band)
            band_groups[band] = self._fid[group]
        return band_groups

    def _read_band_datasets(self):
        bname_fmt = 'BAND{}/DATA'
        band_dsets = {}
        for band in range(1, self.count + 1):
            dset = bname_fmt.format(band)
            band_dsets[band] = self._fid[dset]
        return band_dsets

    @property
    def dtypes(self):
        return self._dtypes

    @property
    def dtype(self):
        return self._dtype

    def _read_dtypes(self):
        dtypes = {}
        for band in self._band_groups:
            bnd_grp = self._band_groups[band]
            val = bnd_grp['DATATYPE'][0]
            dtypes[band] = KEA2NUMPYDTYPE[val]
        return dtypes

    @property
    def no_data(self):
        return self._no_data

    def _read_no_data(self):
        no_data = {}
        for band in self._band_groups:
            bnd_grp = self._band_groups[band]
            val = bnd_grp['NO_DATA_VAL'][0]
            no_data[band] = val
        return no_data

    @property
    def chunks(self):
        return self._chunks

    def _read_chunks(self):
        chunks = {}
        for band in self._band_datasets:
            chunks[band] = self._band_datasets[band].chunks
        return chunks

    @property
    def metadata(self):
        return self._metadata

    def _read_metadata(self):
        metadata = {}
        md = self._fid['METADATA']
        for key in md:
            metadata[key] = md[key][:]
        return metadata

    @property
    def description(self):
        return self._description

    def _read_description(self):
        desc = {}
        for band in self._band_groups:
            bnd_grp = self._band_groups[band]
            val = bnd_grp['DESCRIPTION'][0]
            desc[band] = val
        return desc

    @property
    def layer_useage(self):
        return self._layer_useage

    def _read_layer_useage(self):
        layer_useage = {}
        for band in self._band_groups:
            bnd_grp = self._band_groups[band]
            val = bnd_grp['LAYER_USAGE'][0]
            layer_useage[band] = val
        return layer_useage

    @property
    def layer_type(self):
        return self._layer_type

    def _read_layer_type(self):
        layer_type = {}
        for band in self._band_groups:
            bnd_grp = self._band_groups[band]
            val = bnd_grp['LAYER_TYPE'][0]
            layer_type[band] = val
        return layer_type

    def read(self, bands, window=None):
        """
        """
        # do we have several bands to read
        if isinstance(bands, collections.Sequence):
            nb = len(bands)
            if window is None:
                data = numpy.zeros((nb, self.height, self.width),
                                   dtype=self.dtype)
                for i, band in enumerate(bands):
                    self._band_datasets[band].read_direct(data[i])
            else:
                ys, ye = window[0]
                xs, xe = window[1]
                ysize = ye - ys
                xsize = xe - xs
                idx = numpy.s_[ys:ye, xs:xe]
                data = numpy.zeros((nb, ysize, xsize),
                                   dtype=self.dtypes[0])
                for i, band in enumerate(bands):
                    self._band_datasets[band].read_direct(data[i], idx)
        else:
            if window is None:
                data = self._band_datasets[bands][:]
            else:
                ys, ye = window[0]
                xs, xe = window[1]
                idx = numpy.s_[ys:ye, xs:xe]
                data = self._band_datasets[bands][idx]
                
        return data


class KeaImageReadWrite(KeaImageRead):

    def flush(self):
        """
        """
        self._fid.flush()

    def close(self):
        self.flush()
        self._closed = True
        self._fid.close()

    def set_description(self, band, description):
        """
        """
        # TODO write either fixed length or variable length strings
        dset = self._band_groups[band]['DESCRIPTION']
        dset[0] = description
        self._description[band] = layer_type

    def set_band_metadata(self, band, metadata):
        """
        """

    def set_layer_type(self, band, layer_type=0):
        """
        """
        dset = self._band_groups[band]['LAYER_TYPE']
        dset[0] = layer_type
        self._layer_type[band] = layer_type

    def set_layer_useage(self, band, layer_useage=0):
        """
        """
        dset = self._band_groups[band]['LAYER_USEAGE']
        dset[0] = layer_useage
        self._layer_useage[band] = layer_useage

    def write(self, bands, data, window=None):
        """
        """
        # do we have several bands to write
        if isinstance(bands, collections.Sequence):
            if not set(bands).issubset(self._band_datasets.keys()):
                msg = "1 or more bands does not exist in the output file."
                raise TypeError(msg)

            if data.ndim != 3:
                msg = "Data has {} dimensions and should be 3."
                raise TypeError(msg.format(data.ndim))

            nb = data.shape[0]
            if nb != len(bands):
                msg = "Number of bands, {},  doesn't match data shape, {}."
                raise TypeError(msg.format(len(bands), nb))

            if window is None:
                for i, band in enumerate(bands):
                    dset = self._band_datasets[band]
                    dset[:] = data[i]
            else:
                ys, ye = window[0]
                xs, xe = window[1]
                idx = numpy.s_[ys:ye, xs:xe]
                dset = self._band_datasets[band]
                dset[idx] = data[i]
        else:
            if not set([bands]).issubset(self._band_datasets.keys()):
                msg = "Band {} does not exist in the output file."
                raise TypeError(msg.format(bands))

            if window is None:
                dset = self._band_datasets[bands]
                dset[:] = data
            else:
                ys, ye = window[0]
                xs, xe = window[1]
                idx = numpy.s_[ys:ye, xs:xe]
                dset = self._band_datasets[band]
                dset[idx] = data
