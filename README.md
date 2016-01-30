# geospatial-hdf5

A geospatial interface for h5py.
Support for the KEA image format, + others to come.


Requirements
------------
* NumPy
* h5py 
* affine


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


Parallel (MPI) interface
------------------------

If HDF5 & h5py have been built with the parallel option, then files can be
created with the `MPIO` driver and you workflow can be run using MPI.
See examples/parallel_kea_example.py for an example of an MPI workflow.
See http://docs.h5py.org/en/latest/mpi.html for information on building
parallel HDF5 and h5py.
