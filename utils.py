from random import choices
from string import ascii_lowercase

def random_string(length, pool=ascii_lowercase):
    return ''.join(choices(pool, k=length))