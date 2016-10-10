Examples
--------

The examples listed here detail basic functionality and operation with the KEA file format via geoh5.


Parallel
--------

This presents the simplicity of how to run an MPI program, and output the results to disk.
Using the MPIO driver (if h5py and HDF5 are built in parallel mode), the user doesn't
need to write a whole heap of extra code to synchronise writes, as that is taken care of
by the HDF5 library.
Building parallel [HDF5 & h5py](http://docs.h5py.org/en/latest/mpi.html)
In order for GDAL or a GDAL wrapper such as rasterio, to read the KEA format written in MPIO mode,
all string datasets need to be converted to variable length after the file is written.


Raster Attribute Table
----------------------

The attribute table is read from disk and returned to the user as a `pandas.DataFrame`.
Allowing the user to get busy with data analysis utilising the powerful pandas library.
Read/Write speeds are somewhat quicker than accessing the attribute table via GDAL.


Appending New Bands
-------------------

Appending new bands in the KEA format is very simple, and can be of different data types
compared to existing bands.
New bands can also be a reference/link (think of a UNIX symlink or Window shortcut) to
an already existing band. This is useful for when a raster image such as a rasterised
geometry set is the same through time, but the raster attribute table (RAT) containing
various statistics does change. By using a reference/link, a user doesn't need to
store the same image multiple times in order to have additional RAT's. It saves on
disk space, and makes the file more portable by being smaller.


Additional Compression Filters
------------------------------

Via geoh5, users can write bands and attribute tables using a fast compression filter
called LZF which comes bundled by default with h5py.
In order for GDAL to interpret KEA files with the LZF compression filter, you need
to build LZF as a HDF5 dynamically loaded filter, and set `HDF5_PLUGIN_PATH`
as an environment variable.

Building [LZF](https://github.com/h5py/h5py/tree/master/lzf):
`gcc -O2 -fPIC -shared lzf/*.c lzf_filter.c -o liblzf_filter.so`

Setting an environment variable in BASH
`export HDF5_PLUGIN_PATH=/path/to/filter/plugins`

Other filters plugins can be created added too.
Heavy compression:
[MAFISC](https://wr.informatik.uni-hamburg.de/research/projects/icomex/mafisc)

See [HDFGroup](https://www.hdfgroup.org/services/filters.html) for more filters.
