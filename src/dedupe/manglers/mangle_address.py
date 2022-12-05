import csv
import random
from manglers.mispeller import random_null

# mangle address suffix (e.g. Street, Road, Blvd) by randomly swapping with another version of that suffix
# e.g. 'Street' could be swapped with 'St' or 'St.'
def mangle_address_suffix(address, suffixes, swap_suffix_prob):
    random_value = random.random()
    mangled_address = address

    if random_value < swap_suffix_prob:
        address_parts = address.split(' ')
        orig_suffix = address_parts[-1]
        for suffix_row in suffixes:
            suffix_row_lower = [suffix.lower() for suffix in suffix_row]
            if orig_suffix.lower() in suffix_row_lower:
                different_suffixes = [suffix for suffix in suffix_row_lower if suffix != orig_suffix]
                new_suffix= different_suffixes[random.randint(0, len(different_suffixes)-1)]
                mangled_address = ' '.join(address_parts[:-1]) + ' ' + new_suffix
                break
        
    return mangled_address

# mangle address by randomly swapping the suffix, and randomly setting to blank 
def mangle_address(address, suffixes, swap_suffix_prob, blank_prob):
    mangled_address = mangle_address_suffix(address, suffixes, swap_suffix_prob)
    mangled_address = random_null(mangled_address, blank_prob)

    return mangled_address