from urllib.error import HTTPError
from urllib.request import Request, urlopen
from urllib.parse import urljoin
from cgi import parse_header
import json
from django.conf import settings
from rest_framework import authentication
from rest_framework import exceptions
from rest_framework.authentication import get_authorization_header
import requests

class User(object):
    def __init__(self, id):
        self.__id = id

    def get_id(self):
        return self.__id

    def is_authenticated(self):
        return True

    def __str__(self):
        return "User " + str(self.__id)


class PolicyCompassAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        token = self.__get_token(request)
        if token:
            return User(token), None
        else:
            return None

    def __get_token(self, request):
        auth = get_authorization_header(request).split()

        if not auth or auth[0].lower() != b'token':
            return None

        if len(auth) == 1:
            msg = 'Invalid token header. No credentials provided.'
            raise exceptions.AuthenticationFailed(msg)

        elif len(auth) > 2:
            msg = 'Invalid token header. Token string should not contain spaces.'
            raise exceptions.AuthenticationFailed(msg)

        return auth[1]


class AdhocracyUser:
    def __init__(self, user_ressource_path, is_admin=False, organizations=[]):
        self.resource_path = user_ressource_path
        self.is_admin = is_admin
        self.is_staff = False
        self.organizations = organizations
        self.is_superuser = is_admin
        self.user_permissions = []
        self.groups = []

    def get_username(self):
        return self.resource_path

    def is_authenticated(self):
        return True

    def get_all_permissions(self):
        return self.user_permissions

    def set_password(self, _password):
        raise NotImplementedError

    def check_password(self, _password):
        raise NotImplementedError

    def save(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError

    def __repr__(self):
        return "AdhocracyUser('%s', is_admin=%r, organizations=%v)" % (self.resource_path, self.is_admin, self.organizations)

class AdhocracyAuthenticationPolicyCompass(authentication.BaseAuthentication):
    def authenticate(self, request):
        adhocracy_base_url = settings.PC_SERVICES['references'][
            'adhocracy_api_base_url']
        user_path = request.META.get('HTTP_X_USER_PATH')
        user_token = request.META.get('HTTP_X_USER_TOKEN')
        user_url = urljoin(adhocracy_base_url, user_path)

        if user_path is None and user_token is None:
            return None
        elif user_path is None or user_token is None:
            raise exceptions.AuthenticationFailed(
                'No `X-User-Path` and `X-User-Token` header provided.')

        request = Request(user_url)
        request.add_header('X-User-Path', user_url)
        request.add_header('X-User-Token', user_token)

        try:
            response = urlopen(request)

            content_type, params = parse_header(
                response.getheader("content-type"))
            encoding = params['charset'].lower()
            if content_type != "application/json":
                exceptions.AuthenticationFailed(
                    'Adhocracy authentication failed due to wrong response.')
            resource_as_string = response.read().decode(encoding)
            user_resource = json.loads(resource_as_string)
            roles = user_resource['data'][
                'adhocracy_core.sheets.principal.IPermissions']['roles']
            
            
            oraganizations = []
            
            is_admin = 'admin' in roles
            return (AdhocracyUser(user_path, is_admin, oraganizations), None)

        except HTTPError as e:
            if (e.code == 400):
                raise exceptions.AuthenticationFailed(
                    'Adhocracy authentication failed due to invalid credentials.')
            else:
                raise
            
class AdhocracyAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
                
        cedus_api_base_url = settings.PC_SERVICES['references'][
            'cedus_api_base_url']
        
        user_path = request.META.get('HTTP_X_USER_PATH')
        user_token = request.META.get('HTTP_X_USER_TOKEN')
        # user_url = urljoin(adhocracy_base_url, user_path)
        
        # user_url = 'https://adhocracy-prod.policycompass.eu/api/principals/users/0000003/'
        
        if user_path is None and user_token is None:
            return None
        elif user_path is None or user_token is None:
            raise exceptions.AuthenticationFailed(
                'No `X-User-Path` and `X-User-Token` header provided.')

        # user_url = 'https://account.lab.fiware.org/user?access_token='+user_token
        user_url = cedus_api_base_url+'?access_token='+user_token
        
        # raise exceptions.AuthenticationFailed(user_url)
    
        request = Request(user_url)
        
        # request.add_header('X-User-Path', user_url)
        # request.add_header('X-User-Token', user_token)
        
        
        try:
            

            resp = requests.get(url=user_url)
            
            # raise exceptions.AuthenticationFailed(resp.text)        
            data = json.loads(resp.text)
            # raise exceptions.AuthenticationFailed(data)

            if 'id' in data:
                # raise exceptions.AuthenticationFailed(user_resource)
                if 'roles' in data:
                    roles = data['roles']
                else:                    
                    roles = []
                
                # raise exceptions.AuthenticationFailed(roles)
            else:
                roles = []
                raise exceptions.AuthenticationFailed(data)
            
            
            is_admin = False
            
            for role in roles:               
                #if (role['name']=='admin'):
                if (role['name']=='Seller'):
                    is_admin = True
           
            
            # is_admin = 'admin' in roles            
            
            
            organizations = []
            if 'organizations' in data:
                organizations = data['organizations']
            
            #print ("user organizations")
            #print (organizations)
            
            #return (AdhocracyUser(user_path, is_admin), None) 
            return (AdhocracyUser(user_path, is_admin, organizations), None)   
            


            # return None
        except HTTPError as e:
            if (e.code == 400):
                raise exceptions.AuthenticationFailed(
                    'CEDUS IDM authentication failed due to invalid credentials.')
            else:
                raise
