# Code in this directory was created 
# to support command line testing and 
# bootstrapping of functions originally
# intended for use via the API.
#
# test.py can be used to force the production 
# and publishing to S3 of a full suite of data files 
# such that the public does not get stuck waiting 
# for them to generate on-demand
# 
# audit.py actually can't even be run in this virtualenv
# because it require GeoPandas, but that is too heavy
# a dependency (I think our dokku rejected the VM that included it?)
# It's here because it wasn't clear where else to put it.
 