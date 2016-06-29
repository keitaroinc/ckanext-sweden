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
            text = ''
            
            for v in eval(string):
                str = translations.get(v, v)
                
                if isinstance(str, dict):
                    str = str.get(locale, v)
                
                if isinstance(str, unicode):
                    text += str.encode('utf-8')
                else:
                    text += str
                    
            return text.decode('utf-8')
        
        text = translations.get(string, None)
        
        if text is None:
            return string
        
        return text.get(locale, string)