#!/usr/bin/env python

from distutils.core import setup


setup(name='geoh5',
      version='0.1',
      packages=['geoh5', 'geoh5.kea'],
      scripts=['geoh5/kea/rewrite_strings.py'],
      requires=['numpy', 'h5py', 'affine'],
      install_requires=['numpy', 'h5py>=2.5.0', 'affine'],
      author='Josh Sixsmith',
      maintainer='Josh Sixsmith',
      url='https://github.com/sixy6e/geospatial-hdf5',
      description='A geospatial interface for h5py.',
      license='MIT License (MIT)')
