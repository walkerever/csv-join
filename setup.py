import pathlib
from setuptools import setup,find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="csv-join",
    version="1.0.7",
    description="join csv files in SQL",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/walkerever/csv-join",
    author="Yonghang Wang",
    author_email="wyhang@gmail.com",
    license="Apache 2",
    classifiers=["License :: OSI Approved :: Apache Software License"],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[ "pandas","xtable","sqlalchemy"],
    keywords=[ "csv","query","sql","console" ],
    entry_points={ "console_scripts": 
        [ 
            "csvjoin=csvjoin:csvjoin_main", 
        ] 
    },
)
