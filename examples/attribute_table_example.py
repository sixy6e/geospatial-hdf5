#!/usr/bin/env python

import numpy
from scipy import ndimage
import pandas
from geoh5 import kea
from geoh5.kea import common as kc
# https://github.com/sixy6e/image-processing
from image_processing.segmentation import Segments

"""
Once completed open the file in tuiview to see the colourised segments
and the raster attribute table.
"""

def main():
    """
    Create a segmented array.
    Compute basic stats for each segment:
    (min, max, mean, standard deviation, total, area)
    Write the segmented image and the raster attribute table.
    """

    # data dimensions
    dims = (1000, 1000)
    
    # create some random data and segment via value > 5000
    seg_data = numpy.random.randint(0, 10001, dims).astype('uint32')
    seg_data, nlabels = ndimage.label(seg_data > 5000)
    
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
              'dtype': seg_data.dtype.name}

    with kea.open('file-segments-rat-example.kea', 'w', **kwargs) as src:
        src.write(seg_data, 1)

        # define the layer type as thematic (labelled, classified etc)
        src.write_layer_type(1, kc.LayerType.thematic)

        # write the stats table as an attribute table
        usage = {"Red": "Red",
                 "Green": "Green",
                 "Blue": "Blue",
                 "Alpha": "Alpha",
                 "Histogram": "PixelCount"}
        src.write_rat(stats_table, 1, usage=usage)


if __name__ == '__main__':
    main()
