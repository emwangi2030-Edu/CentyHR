from setuptools import find_packages, setup

setup(
    name="centypack",
    version="0.0.1",
    description="CentyPack packhouse POC for ERPNext v15",
    author="CentyHQ",
    author_email="info@centyhq.com",
    license="MIT",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
)
