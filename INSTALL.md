## Installation

*(This is a work in progress)*

Due to the size of the ACS data we're using, it's relatively difficult to run everything locally for development. In practice we develop by connecting to a remote database on the EC2 instance.

#### Install pre-requisites for local development

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
