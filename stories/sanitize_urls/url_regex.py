# Regular expression for identifying URLs

# protocol identifier (optional) \ # protocol identifier (optional)
# short syntax // still required
url_regex = "(?:(?:(?:https?|ftp):)?\\/\\/)"

# user:pass BasicAuth (optional)
url_regex += \
    "(?:\\S+(?::\\S*)?@)?" + \
    "(?:"

# IP address exclusion
# private & local networks
url_regex += \
      "(?!(?:10|127)(?:\\.\\d{1,3}){3})" + \
      "(?!(?:169\\.254|192\\.168)(?:\\.\\d{1,3}){2})" + \
      "(?!172\\.(?:1[6-9]|2\\d|3[0-1])(?:\\.\\d{1,3}){2})"

# IP address dotted notation octets
# excludes loopback network 0.0.0.0
# excludes reserved space >= 224.0.0.0
# excludes network & broadcast addresses
# (first & last IP address of each class)
url_regex += \
      "(?:[1-9]\\d?|1\\d\\d|2[01]\\d|22[0-3])" + \
      "(?:\\.(?:1?\\d{1,2}|2[0-4]\\d|25[0-5])){2}" + \
      "(?:\\.(?:[1-9]\\d?|1\\d\\d|2[0-4]\\d|25[0-4]))" + \
    "|"

# host & domain names, may end with dot
# can be replaced by a shortest alternative
# (?![-_])(?:[-\\w\\u00a1-\\uffff]{0,63}[^-_]\\.)+
url_regex += \
      "(?:" + \
        "(?:" + \
          "[a-z0-9\\u00a1-\\uffff]" + \
          "[a-z0-9\\u00a1-\\uffff_-]{0,62}" + \
        ")?" + \
        "[a-z0-9\\u00a1-\\uffff]\\." + \
      ")+"

# TLD identifier name, may end with dot
url_regex += \
      "(?:[a-z\\u00a1-\\uffff]{2,}\\.?)" + \
    ")"

# port number (optional)
url_regex += "(?::\\d{2,5})?" 

# resource path (optional)
url_regex += "(?:[/?#]\\S*)?"