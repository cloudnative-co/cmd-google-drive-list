from setuptools import setup, find_packages

setup(
    name="GDriveList",
    version="1.0.5",
    description="List of Google Drive",
    author="sebastian",
    author_email="seba@cloudnative.co.jp",
    packages=find_packages(),
    install_requires=[
        'GoogleAPI @ git+https://git@github.com/cloudnative-co/python-googleapi-sdk'
    ],
    entry_points={
        "console_scripts": [
            'gls = GLS.__init__:main'
        ]
    },
)
