import json
import codecs
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString, box

KOR_BOUNDARY = [124.60970889, 33.11371207, 131.87278407, 38.61370931]

MAX_ENTRY = 100000
ROUND = 4
UNIT = pow(10, -ROUND)

def coord_gen(boundary):
    min_x, y, max_x, max_y = [round(p, ROUND) for p in boundary]
    while y <= max_y:
        x = min_x
        print (f"# {x} {y}")
        while x <= max_x:
            yield Point(x, y) 
            x = round(x + UNIT, ROUND)
        y = round(y + UNIT, ROUND)

def output_opener():
    count = 0
    while True:
        output = codecs.open(f'output/grid_{count:04d}.json', 'w+', encoding = 'utf8')
        count += 1
        yield output

def codes_update(data, codes, code, *indexes):
    for idx in indexes:
        data.update(codes[code[:idx]])

all_area = [
    gpd.GeoDataFrame.from_file('data/EMD_20220324/lon_lat/lon_lat.shp', encoding='utf8'),
    gpd.GeoDataFrame.from_file('data/LI_20220324/lon_lat/lon_lat.shp', encoding='utf8')
    gpd.GeoDataFrame.from_file('data/SIG_20220324/lon_lat/lon_lat.shp', encoding='utf8'),
    gpd.GeoDataFrame.from_file('data/CTPRVN_20220324/lon_lat/lon_lat.shp', encoding='utf8'),
]

area_codes = {
    'CTPRVN_CD': all_alrea[3], 
    'SIG_CD': all_alrea[2], 
    'EMD_CD': all_alrea[1], 
}
codes = {}
for k, v in area_codes.items() :
    result_dict = v.to_dict('records')
    del result_dict['geometry']
    code = result_dict[k]
    codes[code] = result_dict

target_area = all_alrea[:2]

opener = output_opener()
output = next(opener)
count = 0
for p in coord_gen(KOR_BOUNDARY):
    data = {}
    for area in target_area:
        candidates = area.sindex.query(p)
        if candidates.size == 0:
            continue
        candidates = area.iloc[candidates]
        result = candidates[candidates.geometry.contains(p)]
        result_dict = result.drop(columns=['geometry']).to_dict('records')
        if not result_dict:
            continue
        data.update(result_dict[0])
        li_name = ''
        if 'LI_CD' in data:
            codes_update(data, codes, data['LI_CD'] 2, 5, 8)
            li_name = ' ' + data['LI_KOR_NM']
        else:
            codes_update(data, codes, data['EMD_CD'], 2, 5)

        data['KOR_NM'] = "{CTP_KOR_NM} {SIG_KOR_NM} {EMD_KOR_NM}{li_name}".format(**data, li_name = li_name)
        data['latitude'] = p.y
        data['longitude'] = p.x
        print(data)
        if count == MAX_ENTRY:
            output.close()
            output = opener.next()
            count = 0

        json.dump(data, output, ensure_ascii=False)
        output.write('\n')
        count += 1

        break

output.close()
