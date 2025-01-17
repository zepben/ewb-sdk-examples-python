from setuptools import setup, find_namespace_packages

test_deps = ["pytest", "pytest-cov", "pytest-asyncio"]
setup(
    name="zepben.examples",
    description="Module containing examples for interacting with Zepben's platform",
    version="0.2.0b17",
    url="https://github.com/zepben/ewb-sdk-examples-python",
    author="Zeppelin Bend",
    author_email="oss@zepben.com",
    license="MPL 2.0",
    classifiers=[
         "Programming Language :: Python :: 3",
         "Programming Language :: Python :: 3.7",
         "Programming Language :: Python :: 3.8",
         "Programming Language :: Python :: 3.9",
         "Programming Language :: Python :: 3.10",
         "Operating System :: OS Independent"
     ],
    packages=find_namespace_packages(where="src"),
    package_dir={'': 'src'},
    python_requires='>=3.9, <3.12',
    install_requires=[
        "zepben.eas==0.16.0",
        "zepben.evolve==0.43.0b1",
        "numpy==1.26.4",
        "geojson==2.5.0",
        "gql[requests]==3.4.1"
    ],
    extras_require={
        "test": test_deps,
    },
)
