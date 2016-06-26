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


# kea undefined is 0; not sure what the mapping to numpy is
NUMPY2KEADTYPE = {'int8': 1,
                  'int16': 2,
                  'int32': 3,
                  'int64': 4,
                  'uint8': 5,
                  'uin16': 6,
                  'uint32': 7,
                  'uint64': 8,
                  'float32': 9,
                  'float64': 10}

KEA2NUMPYDTYPE = {v: k for k, v in NUMPY2KEADTYPE.items()}

GDAL2KEADTYPE = {gdal.GDT_Unknown: 0,
                 gdal.GDT_Int16: 2,
                 gdal.GDT_Int32: 3,
                 gdal.GDT_Byte: 5,
                 gdal.GDT_UInt16: 6,
                 gdal.GDT_UInt32: 7,
                 gdal.GDT_Float32: 9,
                 gdal.GDT_Float64: 10}

KEA2GDALDTYPE = {v: k for k, v in GDAL2KEADTYPE.items()}

LAYERTYPE = namedtuple('LAYERTYPE', ['CONTINUOUS', 'THEMATIC'])(*range(2))
# LAYERUSEAGE = namedtuple('LAYERUSEAGE',

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
