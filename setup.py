from setuptools import setup

setup(
    name='sapdswsdlclient',
    version='0.1.0',
    description='A Python library for SAP Data Services',
    packages=[
        'sapdswsdlclient',
        'sapdswsdlclient.server',
        'sapdswsdlclient.models',
        'sapdswsdlclient.exceptions',
        'sapdswsdlclient.templates'
    ],
    url='https://github.com/sparklingSky/sap-ds-web-service-client',
    license='Custom Dual License',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: Other/Proprietary License',
    ],
    author='sparklingSky',
    install_requires=['requests~=2.32.3']
)