from api.vendors.worldbank import get
from api.bases.data import Packet

dims = (('country', 'US'), ('indicator', 'AG.AGR.TRAC.NO'))
res = get(dims)
res