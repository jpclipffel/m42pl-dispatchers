from setuptools import setup


setup(
  name='m42pl-dispatchers',
  author='@jpclipffel',
  url='https://github.com/jpclipffel/m42pl-dispatchers',
  version='1.0.0',
  packages=['m42pl_dispatchers', ],
  install_requires=[
      'dill>=0.3.4',
      'psutil>=5.8.0'
    ]
)
