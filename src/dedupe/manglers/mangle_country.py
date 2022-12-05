import manglers.mispeller

# randomly remove/replace characters in name, and randomly set entire name to NULL with given probabilities
def mangle_country(country, remove_prob, replace_prob, null_prob):
    mangle_country = manglers.mispeller.random_null(country, null_prob)
    mangle_country = manglers.mispeller.random_remove(mangle_country, remove_prob)
    mangle_country = manglers.mispeller.random_replace(mangle_country, replace_prob)

    return mangle_country