"""
Defines all routes of the Dataset Manager
"""

from django.conf.urls import patterns, url, include
from .api import *

dataset_urls = patterns(
    '',
    url(r'^/(?P<pk>\d+)$', DatasetDetail.as_view(), name='dataset-detail'),
    url(r'^$', DatasetList.as_view(), name='dataset-list')
)

converter_urls = patterns(
    '',
    url(r'^$', Converter.as_view(), name='converter')

)

cedus_urls = patterns(
    '',
    url(r'^/search$', CedusSearchProxy.as_view(), name='cedus-search'),
    url(r'^/searchcount$', CedusCountProxy.as_view(), name='cedus-search-count'),
    url(r'^/getnodes$', CedusGetNodestProxy.as_view(), name='cedus-get-nodes'),
    url(r'^/download$', CedusDownloadProxy.as_view(), name='cedus-download'),
    url(r'^/organization', CedusOrganization.as_view(), name='cedus-organization'),
    url(r'^/getuserdata', CedusGetUserData.as_view(), name='cedus-getuserdata'),
    url(r'^/dataContextBrocker', CedusPostDataContextBrocker.as_view(), name='cedus-postContextBrockerdata'),
    url(r'^/getEntitiesContextBrocker', CedusGetEntitiesContextBrocker.as_view(), name='cedus-getEntitiesContextBrocker'),
    url(r'^/postSubscriptionContextBrocker', CedusPostSubscriptionContextBrocker.as_view(), name='cedus-postSubscriptionContextBrocker'),
    url(r'^/deleteSubscriptionContextBrocker', CedusDeleteSubscriptionContextBrocker.as_view(), name='cedus-deleteSubscriptionContextBrocker'),
    
)

ckan_urls = patterns(
    '',
    url(r'^/search$', CKANSearchProxy.as_view(), name='ckan-search'),
    url(r'^/download$', CKANDownloadProxy.as_view(), name='ckan-download'),
)

eurostat_urls = patterns(
    '',
    url(r'^/search1$', EurostatSearchStep1Proxy.as_view(), name='eurostat-search1'),
    url(r'^/search2$', EurostatSearchStep2Proxy.as_view(), name='eurostat-search2'),
    url(r'^/download$', EurostatDownloadProxy.as_view(), name='eurostat-download'),
)

urlpatterns = patterns(
    '',
    url(r'^datasets', include(dataset_urls)),
    url(r'^converter', include(converter_urls)),
    url(r'^cedus', include(cedus_urls)),
    url(r'^ckan', include(ckan_urls)),
    url(r'^eurostat', include(eurostat_urls)),
    url(r'^', Base.as_view(), name="dataset-manager-base")
)
