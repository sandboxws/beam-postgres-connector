"""PostgreSQL Apache Beam Connector setup file."""

import os
import subprocess

from setuptools import Command
from setuptools import find_packages
from setuptools import setup

SRC_DIR_NAME = "postgres_connector"
EXAMPLES_DIR_NAME = "examples"
TESTS_DIR_NAME = "tests"


class SimpleCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


class LintCommand(SimpleCommand):
    def run(self):
        subprocess.run(["black", SRC_DIR_NAME, EXAMPLES_DIR_NAME, TESTS_DIR_NAME, "-l", "120", "--check", "--diff"])
        subprocess.run(["mypy", SRC_DIR_NAME, EXAMPLES_DIR_NAME, TESTS_DIR_NAME])


class FormatCommand(SimpleCommand):
    def run(self):
        subprocess.run(["black", SRC_DIR_NAME, EXAMPLES_DIR_NAME, TESTS_DIR_NAME, "-l", "120"])


def get_version():
    global_names = {}
    exec(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "postgres_connector/__init__.py")).read(), global_names)
    return global_names["__version__"]


with open("README.md") as f:
    README = f.read()


PACKAGE_NAME = "beam-postgres-connector"
PACKAGE_VERSION = get_version()
PACKAGE_DESCRIPTION = "PostgreSQL IO Connector for Apache Beam"
PACKAGE_URL = "https://github.com/sandboxws/beam-postgres-connector"
PACKAGE_DOWNLOAD_URL = "https://pypi.python.org/pypi/beam-postgres-connector"
PACKAGE_AUTHOR = "sandboxws"
PACKAGE_EMAIL = "328866+sandboxws@users.noreply.github.com"
PACKAGE_KEYWORDS = "apache beam postgres postgresql connector"
PACKAGE_LONG_DESCRIPTION = README

REQUIRED_PACKAGES = [
    "apache-beam==2.28.0",
    "psycopg2>=2.8.*",
    # "Cython>=0.29.23",
]

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description=PACKAGE_DESCRIPTION,
    long_description=PACKAGE_LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url=PACKAGE_URL,
    download_url=PACKAGE_DOWNLOAD_URL,
    author=PACKAGE_AUTHOR,
    author_email=PACKAGE_EMAIL,
    packages=find_packages(),
    install_requires=REQUIRED_PACKAGES,
    python_requires=">=3.5",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    license="MIT",
    keywords=PACKAGE_KEYWORDS,
    cmdclass=dict(lint=LintCommand, fmt=FormatCommand),
)
