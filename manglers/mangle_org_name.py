import random
import string

# randomly remove x% of characters in name
def random_remove(name, remove_prob):
    if not name: return name
    randomized_name = ''
    for char in name:
        random_value = random.random()
        if random_value >= remove_prob:
            randomized_name += char
    return randomized_name

# randomly replace x% of characters with another lower-case alphabetic character
def random_replace(name, replace_prob):
    if not name: return name
    randomized_name = ''
    for char in name:
        random_value = random.random()
        if random_value < replace_prob:
            next_char = random.choice(string.ascii_lowercase)
        else:
            next_char = char
        randomized_name += next_char
    return randomized_name

# randomly set name to None/NULL x% of the time
def random_null(name, null_prob):
    random_value = random.random()
    if random_value < null_prob:
        return None
    else:
        return name

# randomly remove/replace characters in name, and randomly set entire name to NULL with given probabilities
def mangle_org_name(name, remove_prob, replace_prob, null_prob):
    randomized_name = random_null(name, null_prob)
    randomized_name = random_remove(randomized_name, remove_prob)
    randomized_name = random_replace(randomized_name, replace_prob)

    return randomized_name
    
