import setuptools

setuptools.setup(
    packages=setuptools.find_packages(),
    install_requires=[
        'apache-beam[gcp]==2.64.0',
        'google-cloud-secret-manager==2.22.1',
        'google-cloud-bigquery==3.30.0',
        'google-cloud-logging==3.11.4'
    ],
 )





