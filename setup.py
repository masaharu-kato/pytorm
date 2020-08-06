from setuptools import setup, find_packages

setup(
    name='pytorm',
    version='0.1.0',
    license='',
    description='pytorm',
    author='Masaharu Kato',
    url='https://github.com/masaharu-kato/pytorm',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=['mysql-connector-python'],
    # setup_requires=['pytest-runner'],
    # tests_require=['pytest', 'pytest-cov']
)
