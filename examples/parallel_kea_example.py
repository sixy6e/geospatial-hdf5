#!/usr/bin/env python

from datetime import datetime as dt
import os
from os.path import exists as pexists
from mpi4py import MPI
import numpy
from eotools.tiling import generate_tiles
from geoh5 import kea

def main():
    # processor info
    rank = MPI.COMM_WORLD.rank
    n_proc = MPI.COMM_WORLD.size

    with kea.open('file-3.kea', 'r') as ds:
        chunks = ds.chunks[1]
        # file info
        kwargs = {'width': ds.width,
                  'height': ds.height,
                  'count': 1,
                  'dtype': ds.dtype,
                  'no_data': 0,
                  'chunks': chunks,
                  'blocksize': 250,
                  'parallel': True}

        # create a tiling scheme
        tiles = generate_tiles(int(ds.width), int(ds.height),
                               int(chunks[1]), int(chunks[0]), False)

        # assign a list of unique tiles to each processor
        n_tiles = len(tiles)                                                    
        idx = generate_tiles(n_tiles, 1, n_tiles / n_proc, 1, False)  
        ri = [chunk for _, chunk in idx][rank]                                  
        r_tiles = tiles[ri[0]:ri[1]]

        with kea.open('file-parallel.kea', 'w', **kwargs) as src:
            for tile in r_tiles:
                ytile, xtile = tile
                ysize = ytile[1] - ytile[0]
                xsize = xtile[1] - xtile[0]
                data = ds.read(window=tile)
                result = numpy.log(numpy.mean(data, axis=0)) ** 2
                src.write(result, 1, window=tile)


if __name__ == '__main__':
    rank = MPI.COMM_WORLD.rank
    if not pexists('file-3.kea'):
        if rank == 0:
            kwargs = {'width': 12000,
                      'height': 10000,
                      'count': 6,
                      'dtype': 'float32',
                      'no_data': 0,
                      'chunks': (250, 250),
                      'blocksize': 250,
                      'compression': 1}

            # create a tiling scheme
            tiles = generate_tiles(kwargs['width'], kwargs['height'],
                                   kwargs['chunks'][1], kwargs['chunks'][0])
            
            with kea.open('file-3.kea', 'w', **kwargs) as src:
                for tile in tiles:
                    ytile, xtile = tile
                    ysize = ytile[1] - ytile[0]
                    xsize = xtile[1] - xtile[0]
                    dims = (kwargs['count'], ysize, xsize)
                    data = numpy.random.ranf(dims)
                    bands = range(1, kwargs['count'] + 1)
                    src.write(data, bands, tile)

    if pexists('file-parallel.kea'):
        if rank == 0:
            os.unlink('file-parallel.kea')

    MPI.COMM_WORLD.Barrier()
    st = dt.now()
    main()
    et = dt.now()
    if rank == 0:
        print "Time taken: {}".format(et - st)
