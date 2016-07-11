#!/usr/bin/env python

import numpy
from scipy import ndimage
import pandas
from geoh5 import kea

"""
Once completed open the file in tuiview to see the colourised segments
and the raster attribute table.
"""

def main():
    """
    Write a single band, then append two more bands.
    """

    # data dimensions
    dims = (1000, 1000)
    
    # random data
    data = numpy.random.ranf(dims)
    dtype = data.dtype.name

    # define 1 output band and add other bands later
    kwargs = {'width': dims[1],
              'height': dims[0],
              'count': 1,
              'compression': 4,
              'chunks': (100, 100),
              'blocksize': 100,
              'dtype': dtype}

    with kea.open('append-bands-example.kea', 'w', **kwargs) as src:
        src.write(data, 1)

        # random data
        data = numpy.random.ranf(dims)

        # add a new band to contain the segments data
        src.add_image_band(dtype=dtype, chunks=kwargs['chunks'],
                           blocksize=kwargs['blocksize'], compression=6,
                           band_name='Add One')

        # write the data
        src.write(data, 2)

        # random data
        data = numpy.random.ranf(dims)

        # add a new band to contain the segments data
        src.add_image_band(dtype=dtype, chunks=kwargs['chunks'],
                           blocksize=kwargs['blocksize'], compression=1,
                           band_name='Then Another')

        # write the data
        src.write(data, 3)


if __name__ == '__main__':
    main()
