# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------

import os, subprocess, sys, shlex
project = 'TuGraph'
copyright = '2023, Ant Group'
author = 'Ant Group'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ['myst_parser',
              'sphinx_panels',
              'sphinx.ext.autodoc',
              'sphinx.ext.napoleon',
              'sphinx.ext.viewcode']

# templates_path = ['../../_templates']
exclude_patterns = []


html_theme_options = {
    # 确保此项未被设置为 False 或者类似的隐藏导航栏的设置
    'navigation_depth': -1,
    'globaltoc_collapse': False,
}

source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'

read_the_docs_build = os.environ.get('READTHEDOCS', None) == 'True'
