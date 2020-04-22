#!/usr/bin/env python
from setuptools import setup, find_packages


if __name__ == "__main__":
    setup(name='catch',
          version='0.4.3',
          description=('Planetary Data System Small-Bodies Node astronomical'
                       ' survey search tool.'),
          author="Michael S. P. Kelley",
          author_email="msk@astro.umd.edu",
          url="https://github.com/Small-Bodies-Node/catch",
          packages=find_packages(),
          install_requires=['sbsearch>=1.1.0'],
          setup_requires=['pytest-runner'],
          tests_require=['pytest'],
          scripts=['scripts/catch'],
          license='BSD',
          )
