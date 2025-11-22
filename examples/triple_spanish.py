from mobius.pool import object_28cdad4124395ef2b6ff6d41b605ee1c6d12fcc5cdb5a61407b1f17a9a8499d4 as doble
from mobius.pool import object_d6ecfc908f64e3118d390d922f6dc2354d0a10a1bc2d823994afcccbe2280f02 as sumar

def triplicar(numero):
    """Triplicar un número sumando su doble con él mismo."""
    return sumar(doble(numero), numero)
