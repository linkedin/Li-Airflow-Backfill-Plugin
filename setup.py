import setuptools

version = "1.0.4"

with open('README.md', 'r') as fd:
    long_description = fd.read()

setuptools.setup(
    name='li-airflow-backfill-plugin',
    version=version,
    description='An Airflow Backfill Plugin, from Airflow deployed in LinkedIn Infra production, full-fledged backfill feature with manageability and scalability, including UI and APIs.',
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
