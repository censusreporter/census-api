## Installation

*(This is a work in progress)*

Due to the size of the ACS data we're using, it's relatively difficult to run everything locally for development. In practice we develop by connecting to a remote database on the EC2 instance.

#### Install pre-requisites for local development

The GDAL dependency we rely on for data downloads is a bit complicated to install. We also rely on postgres and memcached.

##### Install build dependencies on a Ubuntu 14.04 machine:

```bash
sudo apt-add-repository -y ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update
sudo apt-get -y install libgdal1-dev libmemcached-dev libpq-dev
export CPLUS_INCLUDE_PATH=/usr/include/gdal
export C_INCLUDE_PATH=/usr/include/gdal
```

##### Install GDAL on a Mac with Homebrew:

```bash
# TBD (Sorry :) )
```

##### Complete Python dependency installation

```bash
mkvirtualenv --no-site-packages census-api
pip install -r requirements.txt
```

#### Set up remote instance

```bash
# Start an EC2 instance

# Set up software on remote instance
fab -i ~/.ssh/censusreporter.ec2_key.pem \
    -u ubuntu \
    -H api.censusreporter.org \
    install_packages initial_config deploy
```
