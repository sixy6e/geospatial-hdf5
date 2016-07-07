#!/usr/bin/env python

from collections import namedtuple
from enum import Enum
import gdal
import h5py


class KeaDataType(Enum):
    undefined = 0
    int8 = 1
    int16 = 2
    int32 = 3
    int64 = 4
    uint8 = 5
    uint16 = 6
    uint32 = 7
    uint64 = 8
    float32 = 9
    float64 = 10
    float = 10


class LayerType(Enum):
    continuous = 0
    thematic = 1


# the layer useage key refers to the band color interp
class BandColourInterp(Enum):
        generic = 0
        greyindex = 1
        paletteindex = 2
        redband = 3
        greenband = 4
        blueband = 5
        alphaband = 6
        hueband = 7
        saturationband = 8
        lightnessband = 9
        cyanband = 10
        magentaband = 11
        yellowband = 12
        blackband = 13
        ycbcr_yband = 14
        ycbcr_cbband = 15
        ycbcr_crband = 16


# raster attribute table field types
class RatFieldTypes(Enum):
    BOOL_FIELDS = 0
    INT_FIELDS = 1
    FLOAT_FIELDS = 2
    STRING_FIELDS = 3


# raster attribute table data types
class RatDataTypes(Enum):
    BOOL = 0
    INT = 1
    FLOAT = 2
    STRING = 3


class NumpyRatTypes(Enum):
    BOOL = 0
    INT8 = 1
    INT16 = 1
    INT32 = 1
    INT64 = 1
    UINT8 = 1
    UINT16 = 1
    UINT32 = 1
    UINT64 = 1
    FLOAT32 = 2
    FLOAT64 = 2
    FLOAT = 2
    OBJECT = 3


# raster attribute table datatypes
ConvertRatDataType = {0: 'int64',
                      1: 'int64',
                      2: 'float64',
                      3: h5py.special_dtype(vlen=str)}


IMAGE_VERSION = "1.2"
VERSION = "1.1"
FILETYPE = "KEA"
GENERATOR = "geoh5"
STRFMT = "S{length}"

# parallel hdf doesn't support variable length types...yet
# see https://www.hdfgroup.org/hdf5-quest.html#pvl
# non-parallel is fine
def fixed_length(val):
    typeid = h5py.h5t.TypeID.copy(h5py.h5t.C_S1)
    typeid.set_size(len(val)+1)
    typeid.set_strpad(h5py.h5t.STR_NULLTERM)
    return typeid
