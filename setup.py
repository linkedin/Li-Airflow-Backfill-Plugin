import setuptools

version = "1.0.2"

with open('README.md', 'r') as fd:
    long_description = fd.read()

setuptools.setup(
    name='li-airflow-backfill-plugin',
    version=version,
    description='Backfill features plugin for Airflow',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/linkedin/Li-Airflow-Backfill-Plugin',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3'
    ],
    package_dir={'': 'plugins'},
    packages=setuptools.find_namespace_packages(where='plugins'),
    include_package_data=True,
    entry_points={
        "airflow.plugins": ["linkedin_backfill = linkedin.airflow.backfill.backfill_plugin:AirflowBackfillPlugin"]
    }
)
