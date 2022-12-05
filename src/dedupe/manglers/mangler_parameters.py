import csv

###################################################
# Define parameters for mangling each field below
###################################################

######## ORGANIZATION NAME ########
name_remove_char_prob = 0.02
name_replace_char_prob = 0.02
name_nullify_prob = 0.02

######## REGION ########
region_remove_char_prob = 0.02
region_replace_char_prob = 0.02
region_nullify_prob = 0.02

######## COUNTRY ########
country_remove_char_prob = 0.02
country_replace_char_prob = 0.02
country_nullify_prob = 0.02

######## ADDRESS ########
address_suffix_swap_prob = 0.33
address_nullify_prob = 0.2


######## URL ########
# some of below probabilities are rough estimates based on organization table
# e.g. around 3% of non-blank URLs in organization table are missing scheme
url_mangling_probs_dict = {
    'www_remove_prob': .22, 
    'scheme_remove_prob': .03, 
    'remove_s_from_https_prob': .68,
    'append_extra_slash_prob': .35,
    'mispell_remove_char_prob': .02, # arbitrary probability
    'mispell_replace_char_prob': .02, # arbitrary probability
    'mispell_null_url_prob': .02 # arbitrary probability
}

# defines probabilities that each TLD key would be converted into another TLD
# NOTE: make sure all probabilities for each TLD key add up to 1!!
tld_swap_prob_dict = {
    'com': {
        'com': 0.90,
        'org': 0.025,
        'net': 0.025,
        'gov': 0.025,
        'edu': 0.025,
        'us': 0
    },
    'org': {
        'com': 0.20,
        'org': 0.80,
        'net': 0,
        'gov': 0,
        'edu': 0,
        'us': 0
    },
    'net': {
        'com': 0.20,
        'org': 0,
        'net': 0.80,
        'gov': 0,
        'edu': 0,
        'us': 0
    },
    'gov': {
        'com': 0.20,
        'org': 0,
        'net': 0,
        'gov': 0.75,
        'edu': 0,
        'us': 0.05
    },
    'edu': {
        'com': 0.20,
        'org': 0,
        'net': 0,
        'gov': 0,
        'edu': 0.80,
        'us': 0
    },
    'us': {
        'com': 0.20,
        'org': 0,
        'net': 0,
        'gov': 0.05,
        'edu': 0,
        'us': 0.75
    }
}