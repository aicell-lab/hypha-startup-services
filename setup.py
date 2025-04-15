import setuptools

# Read dependencies from requirements.txt
with open("requirements.txt", encoding="utf-8") as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="hypha_startup_services",
    version="0.1.1",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=requirements,
)
