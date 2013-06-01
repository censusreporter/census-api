from fabric.api import *
from fabric.contrib.files import *
from fabric.colors import red


def deploy(branch='master'):
    "Deploy the specified branch to the remote host."

    code_dir = 'api_app'
    virtualenv_name = 'api_venv'

    # Install required packages
    sudo('apt-get update')
    sudo('apt-get install -y git')

    # Install up to virtualenv
    sudo('apt-get install -y python-setuptools')
    sudo('easy_install pip')
    sudo('pip install virtualenv')

    # Create virtualenv and add our django app to its PYTHONPATH
    run('virtualenv --no-site-packages %s' % virtualenv_name)

    with settings(warn_only=True):
        if run('test -d %s' % code_dir).failed:
            print(red('Cloning fresh repo.'))
            run('git clone git://github.com/censusreporter/census-extractomatic.git %s' % code_dir)

    with cd(code_dir):
        print(red('Pulling %s from GitHub' % branch))
        run('git pull origin %s' % branch)

        # Install pip requirements
        run('source /home/ubuntu/%s/bin/activate && pip install -r requirements.txt' % virtualenv_name)

        # Run the server
        run('source /home/ubuntu/%s/bin/activate && python simple_api.py' % virtualenv_name)
