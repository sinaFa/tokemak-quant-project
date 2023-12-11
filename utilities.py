import json

def load_abi(file_path):
    with open(file_path, 'r') as abi_file:
        return json.load(abi_file)

def calculate_amounts_agg(row,field1,field2):

    THRESHOLD = 0.1
    token_amounts_a = row[field1]
    token_amounts_b = row[field2]

    if token_amounts_a == 0 and token_amounts_b > 0:
        return token_amounts_b
    elif token_amounts_a > 0 and token_amounts_b == 0:
        return token_amounts_a
    elif token_amounts_a > 0 and token_amounts_b > 0 and abs(token_amounts_a - token_amounts_b)/token_amounts_b < THRESHOLD:
        return token_amounts_b
    elif token_amounts_a == 0 and token_amounts_b == 0:
        return 0
    else:
        return token_amounts_a

