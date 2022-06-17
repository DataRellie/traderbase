
def format_to_binanceapi_pair(three_commas_pair: str):
    parts = three_commas_pair.split('_')
    return f'{parts[1]}{parts[0]}'

def format_to_binanceapi_pairs(three_commas_pairs: list):
    binanceapi_pairs = []
    for three_commas_pair in three_commas_pairs:
        binanceapi_pair = format_to_binanceapi_pair(three_commas_pair)
        binanceapi_pairs.append(binanceapi_pair)
    return binanceapi_pairs

def format_to_three_commas_pair(binanceapi_pair: str, quote: str):
    base = binanceapi_pair.replace(quote, '')
    return f'{quote}_{base}'

def format_to_three_commas_pairs(binanceapi_pairs: list, quote: str):
    three_commas_pairs = []
    for binanceapi_pair in binanceapi_pairs:
        three_commas_pair = format_to_three_commas_pair(binanceapi_pair, quote)
        three_commas_pairs.append(three_commas_pair)
    return three_commas_pairs

def from_to_currencies(three_commas_pair: str):
    return three_commas_pair.split("_")
