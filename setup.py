from setuptools import setup, find_packages

setup(
    name='DaxDashboard',
    version='0.0.0',
    url='https://github.com/bud42/dax-dashboard.git',
    author='Brian D. Boyd',
    author_email='bdboyd42@gmail.com',
    description='Dashboard for DAX using ploty DASH',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'plotly', 'dash', 'dash-core-components',
        'dash-html-components', 'dash-renderer', 'dash-table-experiments'])
