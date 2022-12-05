import manglers.mispeller

# randomly remove/replace characters in name, and randomly set entire name to NULL with given probabilities
def mangle_region(region, remove_prob, replace_prob, null_prob):
    mangled_region = manglers.mispeller.random_null(region, null_prob)
    mangled_region = manglers.mispeller.random_remove(mangled_region, remove_prob)
    mangled_region = manglers.mispeller.random_replace(mangled_region, replace_prob)

    return mangled_region
    
