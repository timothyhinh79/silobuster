from urllib.parse import urlparse # used to extract domain extension
from tld import get_tld # to get domain extension (aka top-level domain or 'TLD') of URL
import random
from manglers.mangle_org_name import random_remove, random_replace, random_null

# consider getting more extensive list of domain extensions, from data or from TLD website
domain_extensions = ['com', 'org', 'net', 'gov', 'edu', 'us', 'co']

# randomly remove www. from URL 
def remove_www(url, prob):
    random_value = random.random()
    mangled_url = url
    if random_value < prob:
        mangled_url = url.replace('www.', '')  
    return mangled_url

# randomly remove scheme from URL
def remove_scheme(url, prob):
    random_value = random.random()
    mangled_url = url
    if random_value < prob:
        mangled_url = url.replace('http://', '').replace('https://','')
    return mangled_url

# randomly remove s from https in URL
def remove_s_from_https(url, prob):
    random_value = random.random()
    mangled_url = url
    if random_value < prob and 'https://' in url:
        mangled_url = url.replace('https://', 'http://')
    return mangled_url

# randomly append extra forward slash (/) to end of URL if it does not already end with one
def append_extra_slash(url, prob):
    random_value = random.random()
    mangled_url = url
    if url and random_value < prob and url[-1] != '/':
        mangled_url = url + '/'
    return mangled_url

# randomly change domain extension
# NOTE: consider adopting more sophisticated method to change domain extensions instead of randomly picking from list
# NOTE: be wary of the replace line...this may accidentally replace another part of the URL?
def change_domain_extension(url, prob):
    random_value = random.random()
    mangled_url = url
    if random_value < prob:
        try:
            tld = get_tld(url)
            diff_domain_extensions = [domain_extension for domain_extension in domain_extensions if domain_extension != tld]
            random_tld = diff_domain_extensions[random.randint(0, len(diff_domain_extensions)-1)]
            mangled_url = url.replace('.' + tld, '.' + random_tld)
        except:
            return mangled_url

    return mangled_url

# randomly remove/replace characters in URL, and randomly set entire URL to NULL with given probabilities
def mispell_url(url, remove_prob, replace_prob, null_prob):
    mangled_url = random_null(url, null_prob)
    mangled_url = random_remove(url, remove_prob)
    mangled_url = random_replace(url, replace_prob)

    return mangled_url

sample_probs_dict = {
    'www_remove_prob': .1,
    'scheme_remove_prob': .1,
    'remove_s_from_https_prob': .1,
    'append_extra_slash_prob': .1,
    'change_domain_ext_prob': .1,
    'mispell_remove_char_prob': .02,
    'mispell_replace_char_prob': .02,
    'mispell_null_url_prob': .02
}

def mangle_url(url, probs_dict):
    mangled_url = remove_www(url, probs_dict['www_remove_prob'])
    mangled_url = remove_scheme(mangled_url, probs_dict['scheme_remove_prob'])
    mangled_url = remove_s_from_https(mangled_url, probs_dict['remove_s_from_https_prob'])
    mangled_url = append_extra_slash(mangled_url, probs_dict['append_extra_slash_prob'])
    mangled_url = change_domain_extension(mangled_url, probs_dict['change_domain_ext_prob'])
    mangled_url = mispell_url(mangled_url, probs_dict['mispell_remove_char_prob'], probs_dict['mispell_replace_char_prob'], probs_dict['mispell_null_url_prob'])

    return mangled_url


