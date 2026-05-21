from setuptools import setup, find_packages

setup(
    name='daxdashboard',
    version='2.0.0',
    url='https://github.com/bud42/dax-dashboard',
    author='Brian D. Boyd',
    author_email='bdboyd42@gmail.com',
    description='Dashboard for DAX using plotly DASH',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'dax',
        'dash',
        'pandas',
        'pycap',
        'dash-bootstrap-components',
        'dash-bootstrap-templates',
        'flask_login',
        'flask_caching',
        'pywebview',
    ],
    include_package_data=True,
)
