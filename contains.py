import sys
import json
import codecs
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import Point, Polygon, LineString, box
from multiprocessing import Pool

KOR_BOUNDARY = [124.60970889, 33.11371207, 131.87278407, 38.61370931]

MAX_ENTRY = 1000000 # 1M
ROUND = 4
UNIT = pow(10, -ROUND)

class Node(object):
    def __init__(self, parent, data):
        self.parent = parent
        self.geometry = data.pop('geometry', None)
        self.data = data

        self.children = []

        if self.parent:
            self.parent.add_child(self)

    def add_child(self, child):
        self.children.append(child)

class OutputWriter(object):
    def __init__(self, idx):
        self.count = 0
        self.opener = OutputWriter.output_opener(idx)
        self.output = next(self.opener)

    @staticmethod
    def output_opener(idx):
        file_count = 0
        while True:
            output = codecs.open(f'output/grid_{idx:02d}_{file_count:04d}.json', 'w+', encoding = 'utf8')
            file_count += 1
            yield output

    def write(self, data, p):
        data['latitude'] = p.y
        data['longitude'] = p.x

        if self.count == MAX_ENTRY:
            self.output.close()
            self.output = next(self.opener)
            self.count = 0

        json.dump(data, self.output, ensure_ascii=False)
        self.output.write('\n')
        self.count += 1

    def close(self):
        self.output.close()

code_nodes = {}

def coord_gen(boundary, concurrency, idx):
    min_x, y, max_x, max_y = [int(round(p * pow(10, ROUND))) for p in boundary]
    all_range = range(y, max_y + 1)
    base = int((max_y - y) / concurrency + 1)
    this_range = [all_range[x:x+base] for x in range(0, len(all_range), base)][idx]
    total = len(this_range)
    count = 0

    for y in this_range:
        print (f"# {idx} y:{round(y * UNIT, ROUND)} {count}/{total}")
        count += 1
        for x in range(min_x, max_x + 1):
            yield Point(round(x * UNIT, ROUND), round(y * UNIT, ROUND)) 
    print (f"### {idx} COMPLETED {count}/{total}")

def codes_update(data, codes, code, *indexes):
    for idx in indexes:
        data.update(codes[code[:idx]])


all_area = [
    ('CTPRVN_CD', 0, False, gpd.GeoDataFrame.from_file('data/CTPRVN_20220324/lon_lat/lon_lat.shp', encoding='utf8')),
    ('SIG_CD', 2, False, gpd.GeoDataFrame.from_file('data/SIG_20220324/lon_lat/lon_lat.shp', encoding='utf8')),
    ('EMD_CD', 5, True, gpd.GeoDataFrame.from_file('data/EMD_20220324/lon_lat/lon_lat.shp', encoding='utf8')),
    ('LI_CD', 8, True, gpd.GeoDataFrame.from_file('data/LI_20220324/lon_lat/lon_lat.shp', encoding='utf8'))
]

for column_key, parent_key_size, keep_geometry, df in all_area :
    for entry in df.to_dict('records'):
        if not keep_geometry:
            del entry['geometry']

        key = entry[column_key]
        parent_key = key[:parent_key_size]
        parent = code_nodes.get(parent_key)
        code_nodes[key] = Node(parent, entry)

target_area = all_area[2]
del all_area 

    
def run(args):
    boundary, concurrency, idx = args
    output = OutputWriter(idx)

    key_column, _, _, area = target_area
    last = [None, None] # Node, data

    for p in coord_gen(boundary, concurrency, idx):
        data = {}

        if last[0] and last[0].geometry.contains(p):
            output.write(last[1], p)
            continue

        candidates = area.sindex.query(p)
        if candidates.size == 0:
            last = [None, None]
            continue

        candidates = area.iloc[candidates]
        result = candidates[candidates.geometry.contains(p)]
        result_dict = result.drop(columns=['geometry']).to_dict('records')
        if not result_dict:
            last = [None, None]
            continue

        data.update(result_dict[0])
        key = data[key_column]
        node = code_nodes[key]
        parent = node.parent
        while parent:
            data.update(parent.data)
            parent = parent.parent
        
        last[0] = node
        li_kor_name = ''
        li_eng_name = ''
        for i,c in enumerate(node.children):
            try:
                if c.geometry.contains(p):
                    data.update(c.data)
                    li_kor_name = ' ' + data['LI_KOR_NM']
                    li_end_name = data['LI_ENG_NM'] + ' '
                    last[0] = c
                    break
            except shapely.errors.TopologicalError as err:
                print(f"{err=} at {c.data}")

        data['ENG_NM'] = "{li_eng_name}{EMD_ENG_NM} {SIG_ENG_NM} {CTP_ENG_NM}".format(**data, li_eng_name = li_eng_name)
        data['KOR_NM'] = "{CTP_KOR_NM} {SIG_KOR_NM} {EMD_KOR_NM}{li_kor_name}".format(**data, li_kor_name = li_kor_name)
        last[1] = data.copy()

        output.write(data, p)
        
    output.close()

if __name__ == '__main__':
    concurrency = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    boundary = KOR_BOUNDARY.copy()
    if len(sys.argv) > 2 :
        boundary[1] = float(sys.argv[2])
    if len(sys.argv) > 3 :
        boundary[3] = float(sys.argv[3])
    if len(sys.argv) > 4 :
        boundary[0] = float(sys.argv[4])
    if len(sys.argv) > 5 :
        boundary[2] = float(sys.argv[5])
    if concurrency == 1 :
        run([boundary, 1, 0])
    else:
        with Pool(concurrency) as pool:
            pool.map(run, [(boundary, concurrency, idx) for idx in range(concurrency)])
