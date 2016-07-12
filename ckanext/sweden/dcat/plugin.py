import json
import requests
import rdflib

from pylons import config

import ckan.plugins as p

from ckanext.dcat.interfaces import IDCATRDFHarvester
from ckanext.sweden.dcat import template_helpers


VALIDATION_SERVICE = 'https://sandbox.oppnadata.se/validator'

# Assume that remote files with this media types are RDF/XML
rdflib.plugin.register(
    'application/octet-stream', rdflib.parser.Parser,
    'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')
rdflib.plugin.register(
    'application/rss+xml', rdflib.parser.Parser,
    'rdflib.plugins.parsers.rdfxml', 'RDFXMLParser')


class SwedenDCATRDFHarvester(p.SingletonPlugin):

    p.implements(IDCATRDFHarvester, inherit=True)
    p.implements(p.IConfigurer)
    p.implements(p.ITemplateHelpers)

    def after_download(self, content, harvest_job):

        if not p.toolkit.asbool(config.get('ckanext.sweden.harvest.use_validation', True)):
            return content, []

        validation_service = config.get('ckanext.sweden.harvest.validation_service', VALIDATION_SERVICE)

        stop_on_errors = p.toolkit.asbool(config.get('ckanext.sweden.harvest.stop_on_validation_errors', False))

        errors = []
        try:
            r = requests.post(validation_service, data=content)
        except requests.exceptions.RequestException, e:
            errors.append(p.toolkit._(
                'Error contacting the validation service: {0}'.format(str(e)))
            )

            if stop_on_errors:
                return None, errors
            else:
                return content, errors

        if r.status_code != 200:

            errors.append(p.toolkit._(
                'The validation service returned an error: {0}'.format(
                    r.status_code)))

            if stop_on_errors:
                return None, errors
            else:
                return content, errors

        else:
            response = r.json()

            if not any([response.get('rdfError'),
                        response.get('errors'),
                        response.get('warnings')]):
                # All clear
                return content, []

            if response.get('rdfError'):
                errors.append(response.get('rdfError'))
            else:
                if response.get('mandatoryError'):
                    for _class in response['mandatoryError']:
                        errors.append(p.toolkit._(
                            'Mandatory class {0} missing'.format(_class)))

                for resource in response.get('resources', []):
                    errors.append(json.dumps(resource))

            if stop_on_errors:
                return None, errors
            else:
                return content, errors

    # IConfigurer
    def update_config(self, config):
        p.toolkit.add_template_directory(config, 'templates')
        p.toolkit.add_resource('fanstatic', 'dcat')

    # ITemplateHelpers
    def get_helpers(self):
        return {
            'json_loads': template_helpers.json_loads,
            'localized_value': template_helpers.get_localized_value
        }
