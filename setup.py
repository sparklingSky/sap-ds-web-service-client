from setuptools import setup

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name='sapdswsdlclient',
    version='0.1.1',
    description='A Python library for SAP Data Services',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['SAP', 'Data Services', 'SOAP', 'WSDL'],
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
        'Programming Language :: Python :: 3.12'
    ],
    author='sparklingSky',
    python_requires='>=3.12',
    install_requires=['requests~=2.32.3']
)