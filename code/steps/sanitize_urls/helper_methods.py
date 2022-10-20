import argparse

def jsonify(keys, key_vals, data_key, sanitized_data):
    json_output = [{key:key_val for key, key_val in zip(keys, key_vals_tuple)} for key_vals_tuple in key_vals]
    for key_vals_dict, sanitized_val in zip(json_output, sanitized_data):
        key_vals_dict[data_key] = sanitized_val
    return json_output

def positive_int(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue