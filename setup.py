from setuptools import setup, find_packages

setup(
    name='DaxDashboard',
    version='1.0.0',
    url='https://github.com/bud42/dax-dashboard.git',
    author='Brian D. Boyd',
    author_email='bdboyd42@gmail.com',
    description='Dashboard for DAX using plotly DASH',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'dax',
        'dash',
        'dash-core-components',
        'dash-html-components',
        'dash-renderer',
        'dash-table',
        'pandas',
        'pycap'])
