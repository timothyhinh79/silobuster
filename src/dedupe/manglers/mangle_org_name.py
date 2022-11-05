import manglers.mispeller

# randomly remove/replace characters in name, and randomly set entire name to NULL with given probabilities
def mangle_org_name(name, remove_prob, replace_prob, null_prob):
    randomized_name = manglers.mispeller.random_null(name, null_prob)
    randomized_name = manglers.mispeller.random_remove(randomized_name, remove_prob)
    randomized_name = manglers.mispeller.random_replace(randomized_name, replace_prob)

    return randomized_name
    
