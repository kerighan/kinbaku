import setuptools

with open("README.md", "r") as f:
    long_description = f.read()


setuptools.setup(
    name="kinbaku",
    version="0.0.2",
    author="Maixent Chenebaux",
    author_email="max.chbx@gmail.com",
    description="Efficient graph database on disk",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kerighan/kinbaku",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=["cachetools"],
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database"
    ],
    python_requires=">=3.7"
)
