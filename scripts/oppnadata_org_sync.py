#!/usr/bin/env python

import os
import sys
import json
import logging

from hashlib import sha1
from datetime import datetime

requires = []
try:
    import ckanapi
except ImportError:
    requires.append('ckanapi')
try:
    import slugify
except ImportError:
    requires.append('slugify')
try:
    import chardet
except ImportError:
    requires.append('chardet')
    
try:
    import requests
except ImportError:
    requires.append('requests')

if requires:
    print('Missing requirements, please install them by running:\n    ' +
          'pip install {0}'.format(' '.join(requires)))
    sys.exit(1)

DATETIME_FORMAT = '%Y-%m-%d %H:%M'
NOW = lambda: datetime.utcnow()
NOW_STR = lambda: datetime.utcnow().strftime(DATETIME_FORMAT)
generate_hash = lambda s: sha1(s).hexdigest()


SITE_ENV_VAR_NAME = 'CKAN_OPPNADATA_SITE_URL'
API_KEY_ENV_VAR_NAME = 'CKAN_OPPNADATA_API_KEY'
ORGS_URL_ENV_VAR_NAME = 'CKAN_OPPNADATA_ORGS_URL'
DEFAULT_EMAIL_ENV_VAR_NAME = 'CKAN_OPPNADATA_DEFAULT_EMAIL'
LOG_PATH_ENV_VAR_NAME = 'CKAN_OPPNADATA_LOG_PATH'


def parse_datetime(string, format=None):
    if format is None:
        format = DATETIME_FORMAT
        
    return datetime.strptime(string, format)

class OppnaDataOrgSync(object):
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is not None:
            print type, value, traceback
            
    def __init__(self):
        # Init logger
        self.log = logging.getLogger(__name__)
        filepath = os.environ.get(LOG_PATH_ENV_VAR_NAME, '/tmp')
        filename = '{0}/oppnadata-org-sync-{1}.log'.format(filepath, NOW_STR())
        self.log.addHandler(logging.StreamHandler(sys.stdout))
        self.log.addHandler(logging.FileHandler(filename))
        self.log.setLevel(logging.DEBUG)
        
        # Init CKAN
        site = os.environ.get(SITE_ENV_VAR_NAME, 'http://oppnadata.se')
        api_key = os.environ.get(API_KEY_ENV_VAR_NAME, '')
        self.ckan = ckanapi.RemoteCKAN(site, apikey=api_key)
        
        self.json_url = os.environ.get(ORGS_URL_ENV_VAR_NAME, 
                                       'https://sandbox.oppnadata.se/sources.json')
        
        self.default_email = os.environ.get(DEFAULT_EMAIL_ENV_VAR_NAME,
                                            'admin@email.com')
        
        
    def _check_unicode(self, text, min_confidence=0.5):
        if not text:
            return None
        
        if isinstance(text, unicode):
            return text.encode('utf-8')
        
        try:
            text = text.decode('utf-8')
        except UnicodeDecodeError:
            guess = chardet.detect(text)
            if guess["confidence"] < min_confidence:
                raise UnicodeDecodeError
            
            text = unicode(text, guess["encoding"])
            text = text.encode('utf-8')
        
        return text
    
    def _validate_org(self, org):
        title = self._check_unicode(org.get('name', None))
        url = self._check_unicode(org.get('url', None))
        dct_url = self._check_unicode(org.get('dct_url', None))
        email = self._check_unicode(org.get('email', None))
        
        if title is None or url is None:
            return False, None
        
        if ' ' in url:
            url = url.replace(' ', '')
        
        if dct_url is not None:
            if ' ' in dct_url:
                dct_url = dct_url.replace(' ', '')
        
        dct_url = dct_url if dct_url is not None \
                            else '{0}/datasets/dcat'.format(url)
                            
        email = email if email is not None \
                        else self.default_email
                        
        name = slugify.slugify(title)
        
        org_obj = {'title': title,
                   'url': url,
                   'name': name,
                   'dcat_url': dct_url,
                   'email': email}
        
        return True, org_obj
    

    def get(self, url, retry=None):
        if retry is None:
            retry = 6
    
        try:
            r = requests.get(url)
        except:
            if retry > 0:
                retry = retry - 1
                return self.get(url, retry)
    
            return None
        return r
    
    def _process_users(self, org, data):
        # Handle users
        if data.get('email'):
            # Check if user is already an admin or has been invited
            user_exists = False
            for org_user in org.get('users', []):
                user = self.ckan.action.user_show(id=org_user['name'])
                if user.get('email') == data.get('email'):
                    user_exists = True
                    self.log.info('User {0} already an admin of "{1}", skipping invite...' \
                            .format(data.get('email'), data.get('title')))
                    break
    
            # Send user invite
            if not user_exists:
                params_invite = {
                    'email': data.get('email'),
                    'group_id': org['id'],
                    'role': 'admin',
                }
    
                user = self.ckan.action.user_invite(**params_invite)
                email_sent = True
                self.log.info('Admin user for "{0}" created, and invite email sent' \
                         .format(data.get('email')))
    
    
    def create_organization(self, data):
        params_org = {
            'title': data.get('title'),
            'name': data.get('name'),
            'extras': [{'key': 'url', 
                        'value': data.get('url')},
                       {'key': 'last_sync', 
                        'value': NOW_STR()},
                       {'key': 'last_sync_hash', 
                        'value': generate_hash(json.dumps(data))},
                       {'key': 'last_sync_dcat_url', 
                        'value': data.get('dcat_url')}],
        }
        
        try:
            org = self.ckan.action.organization_create(**params_org)
        except ckanapi.ValidationError as e:
            self.log.error(str(e))
            return None
        
        org_created = True
        
        self.log.info('Organization "{0}" created'.format(data.get('name')))
            
        # Create/update harvest source
        try:
            source = self.ckan.action.harvest_source_show(id=org['name'],
                                                          url=data.get('dcat_url'))
            self.log.info('Harvest Source "{0}" already exists, perform update...' \
                     .format(data.get('title')))
            
            # Update harvest source url
            harvest_source_params = {'id': data.get('name'),
                                     'owner_org': org['id'],
                                     'url': data.get('dcat_url')}
            
            self.ckan.action.harvest_source_patch(**harvest_source_params)
            self.log.info('Successfully updated harvest source: {0}' \
                          .format(data.get('name')))
            
        except ckanapi.NotFound:
            params_source = {
                'title': org['title'],
                'name': org['name'],
                'url': data.get('dcat_url'),
                'owner_org': org['id'],
                'frequency': 'WEEKLY',
                'source_type': 'dcat_rdf',
            }
            
            source = self.ckan.action.harvest_source_create(**params_source)
            source_created = True
            
            self.log.info('Created harvest source "{0}"'.format(data['title']))
    
            # Create a new harvest job
            params = {
                'source_id': source['id'],
            }
            source = self.ckan.action.harvest_job_create(**params)
            self.log.info('Harvest job "{0}" created'.format(data['title']))
    
        self._process_users(org, data)

    def update_organization(self, data):
        new_hash = generate_hash(json.dumps(data))
        params = {'id': data.get('name'),
                  'extras': [{'key': 'last_sync', 
                              'value': NOW_STR()},
                             {'key': 'last_sync_hash', 
                              'value': new_hash},
                             {'key': 'last_sync_dcat_url', 
                              'value': data.get('dcat_url')}],
        }
        
        org = self.ckan.action.organization_show(id=data.get('name'),
                                                 include_extras=True, 
                                                 all_fields=True)
        
        if 'extras' in org:
            for e in org['extras']:
                if e.get('key') == 'last_sync_hash':
                    if e.get('value') != new_hash:
                        self.log.info('Detected change in organization: {0}, perform update action' \
                                      .format(data.get('title')))
                        
                        params.update({'name': data.get('name'),
                                       'title': data.get('title')})
                        
                        params['extras'].append({'key': 'url', 
                                                 'value': data.get('url')})
                        
                        # Update harvest source url
                        harvest_source_params = {'id': data.get('name'),
                                                 'owner_org': org['id'],
                                                 'url': data.get('dcat_url')}
                        
                        self.ckan.action.harvest_source_patch(**harvest_source_params)
                        self.log.info('Successfully updated harvest source: {0}' \
                                      .format(data.get('name')))
                        
                    else:
                        self.log.info('No change in organization: {0}...skip update!' \
                                      .format(data.get('title')))
                        
        self._process_users(org, data)
        self.ckan.action.organization_patch(**params)
    
    def delete_organization(self, data, soft_delete=False):
        
        if soft_delete:
            self.ckan.action.organization_delete(id=data.get('name'))
            
        else:
            # Unable to purge because fk constraints
            # use delete instead
            dcat_url = None
            for e in data.get('extras', []):
                if e.get('key') != 'last_sync_dcat_url':
                    continue
                
                dcat_url = e.get('value')
                
            dcat_url = dcat_url if dcat_url is not None \
                                    else '{0}/datasets/dcat'.format(data.get('url'))
                                    
            try:
                harvest_source = self.ckan.action.harvest_source_show(url=dcat_url)
            except ckanapi.NotFound:
                
                self.log.info('Harvest source:{0} not found, skipping removal...' \
                              .format(data.get('url')))
            
            else:
                # Clear source (remove all datasets)
                self.ckan.action.harvest_source_clear(id=harvest_source.get('id'))
                
                # Delete source
                self.ckan.action.harvest_source_delete(id=harvest_source.get('id'))
                
                self.ckan.action.package_delete(id=data.get('name'))

            # Check for existing packages
            # and remove them if found
            if data.get('package_count', 0) > 0:
                org = self.ckan.action.organization_show(id=data.get('name'),
                                                         include_datasets=True, 
                                                         all_fields=True)
                for p in org['packages']:
                    self.ckan.action.package_delete(id=p['name'])
                    
            # Delete organization
            self.ckan.action.organization_delete(id=data.get('name'))
            
    def sync(self):
        r = self.get(self.json_url)
        
        if r is None or r.status_code != 200:
            self.log.error('Unable to fetch organization url! Code:{}'.format(r.status_code))
            sys.exit(1)
        
        data = None
        try:
            data = json.loads(r.content)
        except ValueError:
            self.log.error('Unable to parse json response!')
            sys.exit(1)
            
        self.log.debug('Successfully loaded {} organizations...starting synchronization.' \
                 .format(len(data)))

        create, update, delete = [], [], []
        for i, _ in enumerate(data):
            valid, org_dict = self._validate_org(_)
            
            if not valid:
                self.log.warning('Organization: {} is not valid! Skipping...'.format(_))
                continue
            
            # Check if organization exists
            try:
                org = self.ckan.action.organization_show(id=org_dict['name'])
                self.log.debug('Organization "{0}" already exists, perform update action...' \
                         .format(org_dict['name']))
                
                update.append(org_dict)
                
            except ckanapi.NotFound:
                # Create organization
                self.log.debug('Organization "{0}" does not exist, perform create action...' \
                         .format(org_dict['name']))
                
                create.append(org_dict)
                
        # Create organizations
        map(lambda org: self.create_organization(org), create)
        
        # Update organizations
        map(lambda org: self.update_organization(org), update)
        
        # Delete all organizations that are not found in the source json
        params = {'include_extras': True,
                  'all_fields': True}
        
        organization_list = self.ckan.action.organization_list(**params)
        for org in organization_list:
            
            if 'extras' in org:
                found = False
                for e in org['extras']:
                    if e.get('key') != 'last_sync':
                        continue
                    
                    # last_sync date smaller than current date
                    # which means this org wasn't found in the
                    # last sync operation so it should be removed
                    found = True
                    last_sync = parse_datetime(e.get('value'))
                    if last_sync.date() < NOW().date():
                        delete.append(org)
                        break
                    
                if not found:
                    delete.append(org)
                    
            else:
                delete.append(org)
        
        map(lambda org: self.delete_organization(org), delete)
        
        self.log.debug('Create:{0} | Update:{1} | Delete:{2}' \
                       .format(len(create), len(update), len(delete)))
