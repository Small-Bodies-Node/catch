#!/usr/bin/env python
from setuptools import setup, find_packages


if __name__ == "__main__":
    setup(name='catch',
          version='0.0.1',
          description=('Planetary Data System Small-Bodies Node astronomical'
                       ' survey search tool.'),
          author="Michael S. P. Kelley",
          author_email="msk@astro.umd.edu",
          url="https://github.com/Small-Bodies-Node/catch",
          packages=find_packages(),
          requires=['numpy', 'astropy', 'sbsearch'],
          setup_requires=['pytest-runner'],
          tests_require=['pytest'],
          license='BSD',
          )
