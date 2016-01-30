#!/bin/bash

echo "1 CPU"
mpiexec -n 1 python parallel_kea_example.py

# if using the KEA library directly or via GDAL, rasterio, strings written
# into datasets need to be converted from fixed to variable, otherwise
# kealib will crash
FILE=file-parallel.kea
rewrite_strings.py --filename $FILE

echo "4 CPU's"
mpiexec -n 4 python parallel_kea_example.py
rewrite_strings.py --filename $FILE
