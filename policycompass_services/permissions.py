from rest_framework.permissions import BasePermission
from rest_framework import permissions
from .auth import AdhocracyUser


class IsAdhocracyGod(BasePermission):
    """
    Allow only adhocracy users of group god to access
    """

    def has_permission(self, request, view):
        return request.user and type(
            request.user) is AdhocracyUser and request.user.is_admin


class IsCreatorOrReadOnly(BasePermission):
    """
    Allows the Creator of an object and Gods to alter the object
    """
    
    def has_object_permission(self, request, view, obj):
        
        methodsWithPermissions = ['PUT','DELETE']
        
        if request.method in permissions.SAFE_METHODS:
            return True

        if request.user and request.user.is_authenticated():
            if request.user.is_admin:
                return True
            elif hasattr(obj,
                         'creator_path') and obj.creator_path == request.user.resource_path:
                return True
            elif hasattr(obj,
                         'organization') and request.method in methodsWithPermissions:
                
                #print("request.method")
                #print(request.method)
                
                #print("methodsWithPermissions")
                #print(methodsWithPermissions)
                
                returnvalue = False
                #print ("user data")
                #print (request.user.organizations)
                
                #print ("obj.organization")
                #print (obj.organization)
                
                for x in request.user.organizations:
                    #print ("x")
                    #print (x['id'])
                    if (str(x['id'])==str(obj.organization)):
                        #print ("same org ID")
                        for roles in x['roles']:
                            #print ("roles")
                            #print (roles['name'])
                            if (roles['name']=='Seller'):
                                returnvalue = True
                        
                    
                
                #returnvalue = False
                return returnvalue            
            else:
                return False
        else:
            return False
