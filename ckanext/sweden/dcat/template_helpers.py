import json
import os

def json_loads(string):
    try:
        return json.loads(string)
    except ValueError:
        return None


def get_localized_value(string, locale='en'):
    path = os.path.join(os.path.dirname(__file__),
                                       'translations',
                                       'dcat_ap_choices.json')
    
    with open(path) as f:
        
        translations = json.load(f)
        
        if string.startswith('[') and string.endswith(']'):
            return '{}'.format(','.join(translations.get(v, v).get(locale, v) \
                                           for v in eval(string)))
        
        _ = translations.get(string, None)
        if _ is None:
            return string
        
        return _.get(locale, string)
    
    
    