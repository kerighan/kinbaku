**********
Installing
**********


Using pip
---------

To use pip, you need to have `setuptools <https://pypi.python.org/pypi/setuptools>`_ installed.
You can install Kinbaku using the following command:

::

    pip install kinbaku

and an attempt will be made to find and install an appropriate version
that matches your operating system and Python version.

CityHash is the default hashing package of Kinbaku. It is recommended to install the Python package:

::

    pip install cityhash

On windows, you can alternatively install the mmh3 package:

::

    pip install mmh3


GitHub
------

Using pip, you can install the latest version of Kinbaku from the Git source code repository:

::

    pip install git+https://github.com/kerighan/kinbaku.git

Or by cloning the repo and installing from the source:

::

    git clone https://github.com/kerighan/kinbaku.git
    cd kinbaku
    python setup.py install
