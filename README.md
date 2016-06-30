# geospatial-hdf5

A geospatial interface for h5py.
Support for the KEA image format, + others to come.


Requirements
------------
* NumPy
* h5py 
* affine


Installing
----------
[sudo] python setup.py install


KEA format
----------

See http://kealib.org/ for more information


KEA write example
------------

```python
import numpy
from geoh5 import kea

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

with kea.open('file1.kea', 'w', **kwargs) as src:
    src.write(data, bands=range(1, count+1))
```


KEA read/write example
----------------------

```python
from geoh5 import kea

with kea.open('file1.kea', 'r') as src:
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

    with kea.open('file2.kea', 'w', **kwargs) as out_src:
        # Write the first band of data into band 3 on disk, etc..
        out_src.write(data, bands=[3,2,1])
```


KEA read raster attribute table example
---------------------------------------

```python
from geoh5 import kea

ds = kea.open('file3.kea', 'r')

df = ds.read_rat(row_end=10)

print df
```

|     |  Histogram |  Red | Green | Blue | Alpha |         NIR |         RED  |       GREEN |
| --- |  --------: | ---: | ----: | ---: | ----: |        ---: |         ---: |       ----: |
| 0   |        0.0 |    0 |     0 |    0 |   255 |    0.000000 |    0.000000  |    0.000000 |
| 1   | 27056378.0 |   98 |    22 |   91 |   255 | -998.799112 | -998.631636  | -998.348469 |
| 2   |      527.0 |   80 |    82 |   82 |   255 | -999.000000 | -999.000000  | 1154.679317 |
| 3   |      522.0 |  173 |    39 |  234 |   255 | -980.936782 | 1603.070881  | 1203.337165 |
| 4   |      197.0 |  157 |   138 |   35 |   255 | 1797.959391 |  936.984772  |  781.756345 |
| 5   |      167.0 |   74 |     1 |  164 |   255 | 2829.137725 | 1424.215569  | 1256.730539 |
| 6   |      230.0 |  160 |   179 |    8 |   255 |  205.569565 |  287.030435  |  487.886957 |
| 7   |      259.0 |  136 |   119 |  126 |   255 | 2915.200772 | 1609.903475  | 1290.042471 |
| 8   |      114.0 |  130 |   213 |  252 |   255 | 2578.789474 | 1265.815789  | 1033.131579 |
| 9   |      102.0 |  165 |   206 |  207 |   255 | 2748.068627 | 1307.705882  | 1069.568627 |


Parallel (MPI) interface
------------------------

If HDF5 & h5py have been built with the parallel option, then files can be
created with the `MPIO` driver and you workflow can be run using MPI.
See examples/parallel_kea_example.py for an example of an MPI workflow.
See http://docs.h5py.org/en/latest/mpi.html for information on building
parallel HDF5 and h5py.
