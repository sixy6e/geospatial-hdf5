#!/usr/bin/env python

from affine import Affine
import h5py

import collections
import numpy
import pandas

from geoh5.kea import common as kc
from geoh5.kea.common import LayerType
from geoh5.kea.common import BandColourInterp
from geoh5.kea.common import RatFieldTypes
from geoh5.kea.common import RatDataTypes


class KeaImageRead(object):

    """
    The base class for the KEA image format.
    Sets up the `Read` interface.
    """
    
    def __init__(self, fid):
        self._fid = fid
        self._header = None
        self._closed = False
        
        # image dimensions
        self._width = None
        self._height = None
        self._count = None

        # spatial info
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
        self._rat_column_names = None
        self._rat_rows = None
        self._rat_lookup = None

        # do we kick it off???
        # self._read_kea()


    def _read_kea(self):
        self._header = self._read_header()
        self._width = self._header['SIZE'][0]
        self._height = self._header['SIZE'][1]
        self._count = self._header['NUMBANDS'][0]
        self._crs = self._header['WKT'][0]
        self._transform = self._read_transform()
        self._band_groups = self._read_band_groups()
        self._band_datasets = self._read_band_datasets()
        self._dtype, self._dtypes = self._read_dtypes()
        self._no_data = self._read_no_data()
        self._chunks = self._read_chunks()
        self._metadata = self._read_metadata()
        self._description = self._read_description()
        self._layer_useage = self._read_layer_useage()
        self._layer_type = self._read_layer_type()
        self._prep_rat()


    def __enter__(self):
        return self


    # http://docs.quantifiedcode.com/python-anti-patterns/correctness/exit_must_accept_three_arguments.html
    def __exit__(self, exception_type, exception_value, traceback):
        self.close()


    def close(self):
        """
        Closes the HDF5 file.
        """
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
    def crs(self):
        return self._crs


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
        """
        The highest level datatype of each raster band.
        """
        return self._dtype


    def _read_dtypes(self):
        dtypes = {}
        for band in self._band_groups:
            bnd_grp = self._band_groups[band]
            val = bnd_grp['DATATYPE'][0]
            dtypes[band] = kc.KeaDataType(val).name
        dtype = dtypes[1]

        # get the highest level datatype
        # this is used as the base datatype for reading all bands as well as
        # the base datatype for appending a new band.
        for band in dtypes:
            dtype = numpy.promote_types(dtype, dtypes[band])
        return dtype.name, dtypes


    @property
    def no_data(self):
        return self._no_data


    def _read_no_data(self):
        item = 'NO_DATA_VAL'
        no_data = {}
        for band in self._band_groups:
            bnd_grp = self._band_groups[band]
            if item in bnd_grp:
                val = bnd_grp['NO_DATA_VAL'][0]
            else:
                val = None
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
            layer_useage[band] = BandColourInterp(val)
        return layer_useage


    @property
    def layer_type(self):
        return self._layer_type


    def _read_layer_type(self):
        layer_type = {}
        for band in self._band_groups:
            bnd_grp = self._band_groups[band]
            val = bnd_grp['LAYER_TYPE'][0]
            layer_type[band] = LayerType(val)
        return layer_type


    @property
    def rat_column_names(self):
        return self._rat_column_names


    @property
    def rat_rows(self):
        return self._rat_rows


    def _prep_rat(self):
        self._rat_lookup = {}
        self._rat_column_names = {}
        self._rat_rows = {}
        for band in self._band_groups:
            bnd_grp = self._band_groups[band]
            hdr = bnd_grp['ATT/HEADER']
            data = bnd_grp['ATT/DATA']

            # bool, int, float, string fields
            rat_info = hdr['SIZE'][:]
            nrows = rat_info[0]
            rat_fields = rat_info[1:]
            names = range(rat_fields.sum())

            # read the field types
            rat_data = {}
            for i, val in enumerate(rat_fields):
                if val > 0:
                    fname = RatFieldTypes(i).name
                    dname = RatDataTypes(i).name
                    fields = hdr[fname][:]

                    # set column name to link to the dataset, column index,
                    # and the final table index
                    for key in fields:
                        col_name = key[0]
                        col_idx = key[1]
                        tbl_idx = key[-1]
                        rat_data[col_name] = (data[dname], col_idx, tbl_idx)
                        names[tbl_idx] = col_name

            self._rat_lookup[band] = rat_data
            self._rat_column_names[band] = names
            self._rat_rows[band] = nrows


    def read_rat(self, band=1, columns=None, row_start=0, row_end=None):
        """
        Read the raster attribute table for a given band.

        :param bands:
            An integer representing the raster band that the
            raster attribute table should be read from.
            Default is the first band, i.e. `1`.

        :param columns:
            A list of the column names to read. Default is `None`
            in which case all columns are read.

        :param row_start:
            An integer indicating the 1st row to start reading
            from.  Default is 0, the first row (zero based index).

        :param row_end:
            An integer indicating the last row to read up to.
            Default is None, in which case all rows are read.
            The row_end shouldn't exceed the number of rows in
            the table.

        :return:
            A `pandas.DataFrame` containing the raster attribute
            table.
        """
        if band not in range(1, self.count + 1):
            msg = "Invalid band number: {}"
            raise IndexError(msg.format(band))

        # If retrieve for multiple bands, return a pandas panel???
        rat = self._rat_lookup[band]
        valid_cols = self._rat_column_names[band]
        data = {}

        if columns is None:
            # return all columns
            for col in rat:
                dset, idx, tbl_idx = rat[col]
                data[col] = dset[row_start:row_end, idx]
                col_names = self._rat_column_names[band]
        else:
            # check for valid columns
            if not set(columns).issubset(valid_cols):
                msg = ("Invalid column name.\n"
                       "Valid column names are: {}")
                raise IndexError(msg.format(valid_cols))
            col_names = []
            for col in columns:
                dset, idx, tbl_idx = rat[col]
                data[col] = dset[row_start:row_end, idx]
                col_names.append(self._rat_column_names[band][tbl_idx])

        return pandas.DataFrame(data, columns=col_names)


    def read(self, bands=None, window=None):
        """
        Reads the image data into a `NumPy` array.

        :param bands:
            An integer or list of integers representing the
            raster bands that will be read from.
            The length of bands must match the `count`
            dimension of `data`, i.e. (count, height, width).
            If `bands` is None, the default behaviour is to read
            all bands.
        
        :param window:
            A `tuple` containing ((ystart, ystop), (xstart, xstop))
            indices for reading from a specific location within the
            (height, width) 2D image.

        :return:
            A 2D or 3D `NumPy` array depending on whether `bands`
            is a `list` or single integer.
        """
        # default behaviour to read all bands
        if bands is None:
            bands = range(1, self.count + 1)

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
                data = numpy.zeros((nb, ysize, xsize), dtype=self.dtype)
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

    """
    A subclass of `KeaImageRead`.
    Sets up the `Write` interface.
    """

    def flush(self):
        """
        Flushes the HDF5 caches.
        """
        self._fid.flush()


    def close(self):
        """
        Closes the HDF5 file.
        """
        self.flush()
        self._closed = True
        self._fid.close()


    def write_description(self, band, description, delete=True):
        """
        Writes the description for a given raster band.

        :param band:
            An integer representing the band number for which to
            write the description to.

        :param description:
            A string containing the description to be written
            to disk.

        :param delete:
            If set to `True` (default), then the original
            description will be deleted before being re-created.
        """
        # TODO write either fixed length or variable length strings
        if delete:
            del self._band_groups[band]['DESCRIPTION']
            grp = self._band_groups[band]
            grp.create_dataset('DESCRIPTION', shape=(1,), data=description)
        else:
            dset = self._band_groups[band]['DESCRIPTION']
            dset[0] = description
        self._description[band] = description


    def write_band_metadata(self, band, metadata):
        """
        Does nothing yet.
        """
        raise NotImplementedError


    def write_layer_type(self, band, layer_type=LayerType.continuous):
        """
        Writes the layer type for a given raster band.

        :param band:
            An integer representing the band number for which to
            write the description to.

        :param layer_type:
            See class `LayerType`. Default is `LayerType.continuous`.
        """
        dset = self._band_groups[band]['LAYER_TYPE']
        dset[0] = layer_type.value
        self._layer_type[band] = layer_type


    def write_layer_useage(self, band, layer_useage=BandColourInterp.greyindex):
        """
        Writes the layer useage for a given raster band.
        Refers to the colour index mapping to be used for
        displaying the raster band.

        :param band:
            An integer representing the band number for which to
            write the description to.

        :param layer_useage:
            See class `BandColourInterp`.
            Default is `BandColourInterp.greyindex`.
        """
        dset = self._band_groups[band]['LAYER_USEAGE']
        dset[0] = layer_useage.value
        self._layer_useage[band] = layer_useage


    def write(self, data, bands, window=None):
        """
        Writes the image data to disk.

        :param data:
            A 2D or 3D `NumPy` array containing the data to be
            written to disk.

        :param bands:
            An integer of list of integers representing the
            raster bands that will be written to.
            The length of bands must match the `count`
            dimension of `data`, i.e. (count, height, width).
        
        :param window:
            A `tuple` containing ((ystart, ystop), (xstart, xstop))
            indices for writing to a specific location within the
            (height, width) 2D image.
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
                for i, band in enumerate(bands):
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
                dset = self._band_datasets[bands]
                dset[idx] = data


    def add_image_band(self, band_name=None, description=None, dtype='uint8',
                       chunks=(256, 256), blocksize=256, compression=1,
                       shuffle=False, no_data=None):
        """
        Adds a new image band to the KEA file.

        :param band_name:
            If `None` (default), then band name will be `Band {count+1}`
            where `count` is the current nuber of image bands.

        :param description:
            A string containing the image band description. If `None`
            (default) then the description will be an empty string.

        :param dtype:
            A valid `NumPy` style datatype string.
            Defaults to 'uint8'.

        :param chunks:
            A `tuple` containing the desired chunksize for each 2D
            chunk within a given raster band.
            Defaults to (256, 256).

        :param blocksize:
            An integer representing the desired blocksize.
            Defaults to 256.

        :param compression:
            An integer in the range (0, 9), with 0 being low compression
            and 9 being high compression using the `gzip` filter.
            Default is 1. Will be set to `None` when `parallel` is set
            to True.
            The fast compression `lzf` can be used by setting
            `compression='lzf'`.
            Only used when `mode=w'.

        :param shuffle:
            If set to True, then the shuffle filter will be applied
            prior to compression. Higher compression ratio's can be
            achieved by applying the shuffle filter. Default is False.

        :param no_data:
            An integer or floating point value representing the no data or
            fillvalue of the image datasets.
        """

        band_num = self.count + 1
        
        if description is None:
            description = ''

        if band_name is None:
            band_name = 'Band {}'.format(band_num)

        dims = (self.height, self.width)
        kea_dtype = kc.KeaDataType[dtype].value
        gname = 'Band{}'.format(band_num)

        grp = self._fid.create_group(gname)
        grp.create_group('METADATA')
        grp.create_group('OVERVIEWS')

        dset = grp.create_dataset('DATA', shape=dims, dtype=dtype,
                                  compression=compression, shuffle=shuffle,
                                  chunks=chunks, fillvalue=no_data)

        # CLASS 'IMAGE', is a HDF recognised attribute
        dset.attrs['CLASS'] = 'IMAGE'
        dset.attrs['IMAGE_VERSION'] = kc.IMAGE_VERSION

        # image blocksize
        dset.attrs['BLOCK_SIZE'] = blocksize

        # KEA has defined their own numerical datatype mapping
        self._fid[gname].create_dataset('DATATYPE', shape=(1,),
                                        data=kea_dtype, dtype='uint16')

        grp.create_description('DESCRIPTION', shape=(1,), data=description)

        # we'll use a default, but allow the user to overide later
        grp.create_dataset('LAYER_TYPE', shape=(1,), data=0)
        grp.create_dataset('LAYER_USAGE', shape=(1,), data=0)

        # TODO: define a write method for the rat
        # TODO: BOOL_FIELDS, INT_FIELDS, FLOAT_FIELDS, STRING_FIELDS
        grp.create_group('ATT/DATA')

        # TODO need an example in order to flesh the neighbours section
        grp.create_group('ATT/NEIGHBOURS')

        # TODO: option to define the chunksize for the table outputs
        grp.create_dataset('ATT/HEADER/CHUNKSIZE', data=0, dtype='uint64')

        # size is rows then bool, int, float, string columns
        grp.create_dataset('ATT/HEADER/SIZE', data=[0,0,0,0,0], dtype='uint64')

        # do we have no a data value
        if no_data is not None:
            grp.create_dataset('NO_DATA_VAL', shape=(1,), data=no_data)

        dname_fmt = 'Band_{}'.format(band_num)
        md = self._fid['METADATA']
        md.create_dataset(dname_fmt, shape=(1,), data=band_name)

        hdr = self._fid['HEADER']
        hdr['NUMBANDS'][0] = band_num

        self.flush()

        self._read_kea()


    def write_rat(self, dataframe, band, chunksize=1000, compression=1):
        """
        Write a `pandas.DataFrame` as a raster attribute table for a
        given band.

        :param band:

        :param dataframe:
        """
        dtypes = dataframe.dtypes
        columns = dataframe.columns
        nrows, ncols = dataframe.shape

        # the column index will determine the global index to write to disk
        # the order of the column and their datatype will determine
        # the index to write to disk (index local to datatype)
        # what to do about useage (column description)???

        # rat header fields (name, local index, usage, global index)
        # is a composite datatype

        # write each datatype dataset as an expandable along the
        # column axis???

        datatypes = {key.value: [] for key in RatDataTypes}
        for col in columns:
            dtype = dtypes[col].name.upper()
            dvalue = NumpyRatTypes[dtype].value
            datatypes[dvalue].append(col)

        bnd_grp = self._band_groups[band]
        hdr = bnd_grp['ATT/HEADER']
        data = bnd_grp['ATT/DATA']

        # write the chunksize
        if nrows < chunksize:
            chunksize = nrows
        hdr['CHUNKSIZE'] = chunksize

        # write the rat dimensions
        rat_size = hdr['SIZE']
        rat_size[0] = nrows
        for dtype in datatypes:
            # account for the nrows value at idx:0
            rat_size[dtype + 1] = len(datatypes[dtype])

        # create the datasets
        for dtype in datatypes:
            cols = datatypes[dtype]
            ncols_dtype = len(cols)
            if ncols_dtype == 0:
                continue
            out_dtype = ConvertRatDataType[dtype]
            dims = (nrows, ncols_dtype)
            dset = blah.create_dataset(dataset_name, shape=dims,
                                       dtype=out_dtype, chunks=(chunksize, 1),
                                       compression=compression)
            for idx, col in enumerate(cols):
                dset[:, idx] = dataframe[col].values.astype(out_dtype)

        # header fields (name, local index, usage, global index)
        global_idx = columns.get_loc('Histogram')
