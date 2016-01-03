#!/usr/bin/env python

import argparse
from geoh5 import kea


def rewrite_strings(filename):
    """
    Re-writes the string datasets that were written as fixed
    length and converts to variable length.
    This should only need to occur when the file was written
    in parallel/mpi mode.

    :param filename:
        A string containing a full file path name to a KEA formatted
        file written in mpio mode.

    :return:
        None. File is modified in-place.
    """
    with kea.open(filename, mode='r+') as ds:

        # image header information
        hdr = ds._fid['HEADER']
        dims = (1,)

        wkt = bytes(hdr['WKT'][0])
        del hdr['WKT']
        hdr.create_dataset('WKT', shape=dims, data=wkt)

        version = bytes(hdr['VERSION'][0])
        del hdr['VERSION']
        hdr.create_dataset('VERSION', shape=dims, data=version)

        filetype = bytes(hdr['FILETYPE'][0])
        del hdr['FILETYPE']
        hdr.create_dataset('FILETYPE', shape=dims, data=filetype)

        gen = bytes(hdr['GENERATOR'][0])
        del hdr['GENERATOR']
        hdr.create_dataset('GENERATOR', shape=dims, data=gen)

        ds.flush()

        # band descriptions
        for i in range(1, ds.count + 1):
            description = ds.description[i]
            ds.write_description(i, description)

        ds.flush()

        # image metadata
        md = ds._fid['METADATA']
        for key in md:
            data = bytes(md[key][0])
            dims = md[key].shape
            del md[key]
            md.create_dataset(key, shape=dims, data=data)

        ds.flush()


if __name__ == '__main__':
    desc = "Re-writes the fixed length string datasets to variable length."
    hlp = 'The filename to a KEA image formatted HDF5 file.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--filename', required=True, help=hlp)

    args = parser.parse_args()

    rewrite_strings(args.filename)
