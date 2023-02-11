from data.vendors.worldbank import get
from data.bases.data import Packet

dims = (('country', 'US'), ('indicator', 'AG.AGR.TRAC.NO'))
res = get(dims)
res