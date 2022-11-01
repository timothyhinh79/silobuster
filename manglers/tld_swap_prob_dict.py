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