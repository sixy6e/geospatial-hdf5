#!/usr/bin/env python

import numpy
from geoh5 import kea
from geoh5.kea import common as kc
# https://github.com/sixy6e/image-processing
from image_processing.segmentation import Segments


def main():

    # data dimensions
    dims = (1000, 1000)
    
    # create some data; we'll use the integers themselves as segment id's
    seg_data = numpy.random.randint(0, 10001, dims)
    
    # create some random data to calculate stats against
    data = numpy.random.ranf(dims)
    
    # create a segments class object
    seg = Segments(seg_data, include_zero=True)
    
    # retrieve basic stats (min, max, mean, standard deviation, total, area)
    stats_table = seg.basic_statistics(data, dataframe=True)
    stats_table.set_index("Segment_IDs", inplace=True)
    
    # join via segment id, specifying 'outer' will account for empty segments
    df = pandas.DataFrame({"Histogram": seg.histogram})
    stats_table = df.join(stats_table, how='outer')
    nrows = stats_table.shape[0]

    # assign random colours to each segment
    stats_table.insert(1, "Red", numpy.random.randint(0, 256, (nrows)))
    stats_table.insert(2, "Green", numpy.random.randint(0, 256, (nrows)))
    stats_table.insert(3, "Blue", numpy.random.randint(0, 256, (nrows)))
    stats_table.insert(4, "Alpha", 255)

    # define 1 output band and add another band later
    kwargs = {'width': dims[1],
              'height': dims[0],
              'count': 1,
              'compression': 4,
              'chunks': (100, 100),
              'blocksize': 100,
              'dtype': data.dtype.name}

    with kea.open('file-segments-rat-example.kea', 'w', **kwargs) as src:
        src.write(data, 1)

        # add a new band to contain the segments data
        src.add_image_band(dtype=seg_data.dtype.name, chunks=kwargs['chunks'],
                           blocksize=kwargs['blocksize'], compression=3,
                           band_name='Segments')

        # define the layer type as thematic (labelled, classified etc)
        src.write_layer_type(2, kc.LayerType.thematic)

        # write the segmented (classified) array
        src.write(seg_data, 2)

        # write the stats table as an attribute table
        src.write_rat(stats_table, 2)
