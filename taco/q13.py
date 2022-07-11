import pytaco as pt
from pytaco import dense, compressed
from fastparquet import ParquetFile

# cn.id
pf = ParquetFile('myfile.parq')
df = pf.to_pandas()

cn = pt.tensor([234997], compressed)
# ct.id
ct = pt.tensor([4], compressed)
# it.id
it = pt.tensor([113], compressed)
# it.id
it2 = pt.tensor([113], compressed)
# kt.id
kt = pt.tensor([7], compressed)
# mc.movie_id mc.company_type_id mc.company_id
mc = pt.tensor([2528312, 4, 234997], compressed)
# mi.movie_id mi.info_type_id
mi = pt.tensor([2528312, 113], compressed)
# miidx.movie_id miidx.info_type_id
miidx = pt.tensor([2528312, 113], compressed)
# t.id t.kind_id
t = pt.tensor([2528312, 7], compressed)

q = pt.tensor(0)

cn.insert([0], 1)

cn.pack()

i = pt.get_index_vars(1)

q[None] = cn[i]

for coordinates, val in q:
  print(val)
