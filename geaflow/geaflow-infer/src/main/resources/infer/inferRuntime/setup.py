# -*-coding:utf-8-*-
# cython:language_level=3

from distutils.core import setup
from Cython.Build import cythonize

setup(ext_modules=cythonize("mmap_ipc.pyx"))
