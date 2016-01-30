#!/bin/bash

echo "1 CPU"
mpiexec -n 1 python parallel_kea_example.py

# if using the KEA library directly or via GDAL, rasterio, strings written
# into datasets need to be converted from fixed to variable, otherwise
# kealib will crash
PYFILE=../geoh5/kea/rewrite_strings.py
FILE=file-parallel.kea

python $PYFILE --filename $FILE

echo "4 CPU's"
mpiexec -n 4 python parallel_kea_example.py
python $PYFILE --filename $FILE
