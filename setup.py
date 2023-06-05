from setuptools import setup, find_namespace_packages

test_deps = ["pytest", "pytest-cov", "pytest-asyncio"]
setup(
    name="zepben.examples",
    description="Module containing examples for interacting with Zepben's platform",
    version="0.2.0b8",
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
    python_requires='>=3.7',
    install_requires=[
        "zepben.auth==0.10.0",
        "zepben.eas==0.10.0",
        "zepben.evolve==0.35.0b17",
        "zepben.edith==0.3.0b4",
        "pp-translator==0.7.0b3",
        "numba==0.56.4",
        "geojson==2.5.0",
        "graphqlclient==0.2.4"
    ],
    extras_require={
        "test": test_deps,
    },
)

