from fabric.api import *
from fabric.contrib.files import *
from fabric.context_managers import shell_env, prefix
from fabric.colors import green

root_dir = '/home/www-data'
code_dir = '%s/api_app' % root_dir
virtualenv_name = 'api_venv'
virtualenv_dir = '%s/%s' % (root_dir, virtualenv_name)

def _download_sql_backups(data_to_load):
    """ Creates a new AWS EBS snapshot.

        `data_to_load`: A list of releases to import into the database. """

    possible_data_sources = {
        'tiger2012':   'https://s3.amazonaws.com/census-backup/tiger/2012/tiger2012_backup.sql.gz',
        'acs2012_1yr': 'http://census-backup.s3.amazonaws.com/acs/2012/acs2012_1yr/acs2012_1yr_backup.sql.gz',
        'acs2012_3yr': 'http://census-backup.s3.amazonaws.com/acs/2012/acs2012_3yr/acs2012_3yr_backup.sql.gz',
        'acs2012_5yr': 'http://census-backup.s3.amazonaws.com/acs/2012/acs2012_5yr/acs2012_5yr_backup.sql.gz',
        'acs2011_1yr': 'http://census-backup.s3.amazonaws.com/acs/2011/acs2011_1yr/acs2011_1yr_backup.sql.gz',
        'acs2011_3yr': 'http://census-backup.s3.amazonaws.com/acs/2011/acs2011_3yr/acs2011_3yr_backup.sql.gz',
        'acs2011_5yr': 'http://census-backup.s3.amazonaws.com/acs/2011/acs2011_5yr/acs2011_5yr_backup.sql.gz',
        'acs2010_1yr': 'http://census-backup.s3.amazonaws.com/acs/2010/acs2010_1yr/acs2010_1yr_backup.sql.gz',
        'acs2010_3yr': 'http://census-backup.s3.amazonaws.com/acs/2010/acs2010_3yr/acs2010_3yr_backup.sql.gz',
        'acs2010_5yr': 'http://census-backup.s3.amazonaws.com/acs/2010/acs2010_5yr/acs2010_5yr_backup.sql.gz',
        'acs2009_1yr': 'http://census-backup.s3.amazonaws.com/acs/2009/acs2009_1yr/acs2009_1yr_backup.sql.gz',
        'acs2009_3yr': 'http://census-backup.s3.amazonaws.com/acs/2009/acs2009_3yr/acs2009_3yr_backup.sql.gz',
        'acs2008_1yr': 'http://census-backup.s3.amazonaws.com/acs/2008/acs2008_1yr/acs2008_1yr_backup.sql.gz',
        'acs2008_3yr': 'http://census-backup.s3.amazonaws.com/acs/2008/acs2008_3yr/acs2008_3yr_backup.sql.gz',
        'acs2007_1yr': 'http://census-backup.s3.amazonaws.com/acs/2007/acs2007_1yr/acs2007_1yr_backup.sql.gz',
        'acs2007_3yr': 'http://census-backup.s3.amazonaws.com/acs/2007/acs2007_3yr/acs2007_3yr_backup.sql.gz',
    }

    files_to_download = []
    for release in data_to_load:
        url = possible_data_sources.get(release)
        if not url:
            raise Exception('The specified data \'%s\' is unrecognized.' % release)
        files_to_download.append(url)

    sudo('mkdir -p /mnt/tmp')
    sudo('chown -R ubuntu /mnt/tmp')

    for url in data_to_load:
        print(green('Downloading to host: ' + url))
        run('wget --quiet --continue --directory-prefix="/mnt/tmp" ' + url)
    print(green('Done downloading source data!'))

def _install_base():
    """ Update apt-get and install base packages (like git). """
    sudo('apt-get update -q && sudo apt-get upgrade -q -y')

    # Install the python dev packages
    sudo('apt-get install -q -y git libpq-dev python-dev libmemcached-dev build-essential libgdal1-dev')

def _mount_ebs():
    """ Install the XFS support tools and mount the EBS volume. """
    sudo('apt-get install -q -y xfsprogs')

    sudo('mkfs.xfs /dev/xvdc', warn_only=True)
    append('/etc/fstab', "/dev/xvdc /vol xfs noatime 0 0", use_sudo=True)
    sudo('mkdir -p -m 000 /vol')
    sudo('mount /vol', warn_only=True)

def _install_postgres():
    """ Install PostgreSQL and PostGIS. """

    sudo('apt-get install -q -y postgresql-9.1 postgresql-9.1-postgis')
    sudo('/etc/init.d/postgresql stop') # Stop it so we can move the data dir
    sudo('mkdir -p /vol/postgresql')
    if not exists('/vol/postgresql/9.1'):
        sudo('mv /var/lib/postgresql/9.1 /vol/postgresql/')
    sudo('chown -R postgres:postgres /vol/postgresql')
    sudo("sed -i \"s/data_directory = '\/var\/lib\/postgresql\/9.1\/main'/data_directory = '\/vol\/postgresql\/9.1\/main'/\" /etc/postgresql/9.1/main/postgresql.conf")
    sudo('/etc/init.d/postgresql start')

    # Create PostgreSQL `census` user and database
    sudo('psql -c "CREATE ROLE census WITH NOSUPERUSER LOGIN UNENCRYPTED PASSWORD \'censuspassword\';"', user='postgres', warn_only=True)
    sudo('psql -c "CREATE DATABASE census WITH OWNER census;"', user='postgres', warn_only=True)

    # Make PostgreSQL login password-less
    if not exists('/home/ubuntu/.pgpass'):
        append('/home/ubuntu/.pgpass', 'localhost:5432:census:census:censuspassword')
        run('chmod 0600 /home/ubuntu/.pgpass')

def _install_libgdal():
    """ Install the latest libgdal-dev package. """
    # The one included in base Ubuntu doesn't appear to be new enough any more.
    sudo('apt-add-repository -y ppa:ubuntugis/ubuntugis-unstable')
    sudo('apt-get -q update')
    sudo('apt-get -q -y install libgdal1-dev')

def _install_elasticsearch():
    """ Install and start ElasticSearch. """
    sudo('apt-get install -q -y openjdk-7-jre-headless')
    run('wget --quiet --continue https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.0.1.deb')
    sudo('dpkg -i elasticsearch-1.0.1.deb')
    sudo('mkdir -p /vol/elasticsearch')
    sudo('chown elasticsearch:elasticsearch /vol/elasticsearch')
    append('/etc/elasticsearch/elasticsearch.yml', 'path.data: /vol/elasticsearch', use_sudo=True)
    sudo('service elasticsearch restart')

def _install_memcached():
    """ Install and start memcached. """
    sudo('apt-get install -q -y memcached')

def _install_apache():
    """ Install and set up apache and mod_wsgi. """
    sudo('apt-get install -q -y apache2 libapache2-mod-wsgi')
    sudo('a2enmod wsgi', warn_only=True)

def install_packages():
    """ Installs OS packages required to run the API. """
    _install_base()
    _mount_ebs()
    _install_postgres()
    _install_libgdal()
    _install_elasticsearch()
    _install_memcached()
    _install_apache()

def flushcache():
    "Flush the memcache by restarting it."

    sudo('service memcached restart')

def initial_config():
    """ Configure the remote host to run Census Reporter API. """

    host = 'api.censusreporter.org'

    sudo('mkdir -p %s' % root_dir)
    sudo('chown www-data:www-data %s' % root_dir)

    sudo('rm -f /etc/apache2/sites-enabled/000-default')
    sudo('rm -f /etc/apache2/sites-enabled/%s' % host)
    sudo('rm -f /etc/apache2/sites-available/%s' % host)
    upload_template('./server/apache2/site', '/etc/apache2/sites-available/%s' % host, use_sudo=True, context={
        'domainname': host,
        'project_path': code_dir,
        'wsgi_path': '%s/census_extractomatic/api.wsgi' % (code_dir),
        'venv_path': '%s/lib/python2.7/site-packages' % (virtualenv_dir),
    })
    sudo('a2ensite %s' % host)

    # Install up to virtualenv
    sudo('apt-get install -q -y python-setuptools')
    sudo('easy_install pip')
    sudo('pip install virtualenv')

    # Create virtualenv and add our django app to its PYTHONPATH
    sudo('virtualenv %s' % virtualenv_dir, user='www-data')
    sudo('rm -f %s/lib/python2.7/site-packages/censusreporter.pth' % virtualenv_dir, user='www-data')
    append('%s/lib/python2.7/site-packages/censusreporter.pth' % virtualenv_dir, code_dir, use_sudo=True)
    sudo('chown www-data:www-data %s/lib/python2.7/site-packages/censusreporter.pth' % virtualenv_dir)

    with settings(warn_only=True):
        if sudo('test -d %s' % code_dir, user='www-data').failed:
            sudo('git clone git://github.com/censusreporter/census-api.git %s' % code_dir, user='www-data')

def deploy(branch='master'):
    """ Deploy the specified Census Reporter API branch to the remote host. """

    with cd(code_dir):
        sudo('find . -name \'*.pyc\' -delete', user='www-data')
        sudo('git pull origin %s' % branch, user='www-data')

        # Install pip requirements
        with prefix('source %s/bin/activate' % virtualenv_dir):
            with shell_env(CPLUS_INCLUDE_PATH='/usr/include/gdal', C_INCLUDE_PATH='/usr/include/gdal'):
                sudo('pip install -r requirements.txt', user='www-data')

    # Restart apache
    sudo('service apache2 restart')

def load_elasticsearch_data(releases=['acs2012_1yr', 'acs2012_3yr', 'acs2012_5yr'], delete_first=False):
    """ Loads search data into our ElasticSearch index. """

    # The table index data lives in our census-table-metadata repo
    if not exists('/home/ubuntu/census-table-metadata'):
        with cd('/home/ubuntu'):
            run('git clone https://github.com/censusreporter/census-table-metadata.git')

    if delete_first:
        # Delete any existing data
        run("curl -XDELETE 'http://localhost:9200/census/'")

    # Bulk-insert the data
    with cd('/home/ubuntu/census-table-metadata'):
        for release in releases:
            run("curl -S --output /dev/null -XPOST 'http://localhost:9200/_bulk' --data-binary @precomputed/%s/census_column_metadata.txt" % release)
            run("curl -S --output /dev/null -XPOST 'http://localhost:9200/_bulk' --data-binary @precomputed/%s/census_table_metadata.txt" % release)

def load_postgresql_data(releases=['acs2012_1yr', 'acs2012_3yr', 'acs2012_5yr', 'tiger2012'], delete_first=False):
    """ Loads Census data (including metadata) from the specified releases into PostgreSQL. """

    sudo("psql -d census -c \"COPY public.census_tabulation_metadata FROM '/home/ubuntu/census-table-metadata/precomputed/unified_metadata.csv' WITH csv ENCODING 'utf8' HEADER;\"", user='postgres')

    print "THIS IS INCOMPLETE. I'm only loading a tabulation metadata for now."
