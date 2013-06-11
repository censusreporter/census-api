from fabric.api import *
from fabric.contrib.files import *
from fabric.colors import red


def deploy(branch='master'):
    "Deploy the specified branch to the remote host."

    root_dir = '/home/www-data'
    code_dir = '%s/api_app' % root_dir
    virtualenv_name = 'api_venv'
    virtualenv_dir = '%s/%s' % (root_dir, virtualenv_name)
    host = 'api.censusreporter.org'

    sudo('mkdir -p %s' % root_dir)
    sudo('chown www-data:www-data %s' % root_dir)

    # Install required packages
    sudo('apt-get update')
    sudo('apt-get install -y git libpq-dev python-dev')

    # Install and set up apache and mod_wsgi
    sudo('apt-get install -y apache2 libapache2-mod-wsgi')
    sudo('a2enmod wsgi')
    sudo('rm -f /etc/apache2/sites-enabled/000-default')
    sudo('rm -f /etc/apache2/sites-enabled/%s' % host)
    sudo('rm -f /etc/apache2/sites-available/%s' % host)
    upload_template('./server/apache2/site', '/etc/apache2/sites-available/%s' % host, use_sudo=True, context={
        'domainname': host,
        'project_path': '%s/census_extractomatic' % (code_dir),
        'venv_path': '%s/lib/python2.7/site-packages' % (virtualenv_dir),
    })
    sudo('a2ensite %s' % host)

    # Install up to virtualenv
    sudo('apt-get install -y python-setuptools')
    sudo('easy_install pip')
    sudo('pip install virtualenv')

    # Create virtualenv and add our django app to its PYTHONPATH
    sudo('virtualenv --no-site-packages %s' % virtualenv_dir)
    sudo('rm -f %s/lib/python2.7/site-packages/censusreporter.pth' % virtualenv_dir)
    append('%s/lib/python2.7/site-packages/censusreporter.pth' % virtualenv_dir, '%s/census_extractomatic' % code_dir, use_sudo=True)

    with settings(warn_only=True):
        if sudo('test -d %s' % code_dir).failed:
            sudo('git clone git://github.com/censusreporter/census-extractomatic.git %s' % code_dir)

    with cd(code_dir):
        sudo('git pull origin %s' % branch)

        # Install pip requirements
        sudo('source %s/bin/activate && pip install -r requirements.txt' % virtualenv_dir)

        # Make sure everything is correctly owned
        sudo('chown www-data:www-data -R %s %s' % (code_dir, virtualenv_dir))

    # Restart apache
    sudo('service apache2 restart')
