__author__ = 'fki'

from .settings_basic import *

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'pcompass.db',
    }
}

# Database config example for PostgreSQL

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.postgresql_psycopg2',
#         'NAME': 'pcompass',
#         'USER': 'pcompass',
#         'PASSWORD': 'password',
#         'HOST': 'localhost',
#     }
# }

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True
#TEMPLATE_DEBUG = True

ELASTICSEARCH_URL = 'http://localhost:9200/policycompass_search/'

PC_SERVICES = {
    'IDM': {
        'DEFAULT_USERNAME': 'atos',
        'DEFAULT_PASSWORD': 'atos',
        'URL_TOKEN': 'http://idm.cedus.eu:5000/v3/auth/tokens',
        'URL_ORGANIZATION': 'http://idm.cedus.eu:5000/v3/OS-SCIM/v2/Organizations'  
     },               
    'references': {
        'cedus_api_base_url_old': 'http://217.172.12.177:8000/user',
        'cedus_api_base_url': 'http://idm.cedus.eu/user',                        
        'frontend_base_url': 'http://localhost:9000',
        'MEDIA_URL' : 'media/',
        'base_url': 'http://localhost:8000',
        'units': '/api/v1/references/units',
        'external_resources': '/api/v1/references/externalresources',
        'languages': '/api/v1/references/languages',
        'domains': '/api/v1/references/policydomains',
        'dateformats': '/api/v1/references/dateformats',
        'eventsInVisualizations': '/api/v1/visualizationsmanager/eventsInVisualizations',
        #'metricsInvisualizations': '/api/v1/visualizationsmanager/metricsInVisualizations',
        'datasetsInvisualizations': '/api/v1/visualizationsmanager/datasetsInVisualizations',
        'updateindexitem' : '/api/v1/searchmanager/updateindexitem',
        'deleteindexitem' : '/api/v1/searchmanager/deleteindexitem',
        'fcm_base_url': 'http://localhost:8080',
        'adhocracy_api_base_url': 'http://localhost:6541',
        'eventminer_url': 'http://localhost:5000/extraction'
    },
    'external_resources': {
        'physical_path_phantomCapture': '/home/miquel/PolicyCompass/policycompass/backendv2/policycompass-services/apps/visualizationsmanager/phantomCapture/main.js',    
    }
}
