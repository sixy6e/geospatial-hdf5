#!/usr/bin/env python

import numpy
from geoh5 import kea


def main():
    # create some data
    data = numpy.random.randint(0, 256, (6, 100, 100)).astype('uint8')
    
    count, height, width = data.shape
    kwargs = {'width': width,
              'height': height,
              'count': count,
              'dtype': data.dtype.name,
              'compression': 2,
              'no_data': 0,
              'chunks': (25, 25),
              'blocksize': 25}
    
    # write to disk
    with kea.open('file-1.kea', 'w', **kwargs) as src:
        src.write(data, bands=range(1, count+1))

    # re-open as a new file object
    with kea.open('file-1.kea', 'r') as src:
        # Read the first band
        src.read(1)
    
        # Read bands [4, 3, 2] and return in that order
        data = src.read([4,3,2])
    
        kwargs = {'width': src.width,
                  'height': src.height,
                  'count': 3,
                  'transform': src.transform,
                  'crs': src.crs,
                  'compression': 4,
                  'no_data': src.no_data[1],
                  'chunks': (50, 50),
                  'blocksize': 50,
                  'dtype': src.dtype}
    
        # create a new output file
        with kea.open('file-2.kea', 'w', **kwargs) as out_src:
            # Write the first band of data into band 3 on disk, etc..
            out_src.write(data, bands=[3,2,1])


if __name__ == '__main__':
    main()
