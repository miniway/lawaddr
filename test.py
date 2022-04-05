import sys
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString, box

lat = float(sys.argv[1])
lon = float(sys.argv[2])

all_area = [
    gpd.GeoDataFrame.from_file('data/CTPRVN_20220324/lon_lat/lon_lat.shp', encoding='utf8'),
    gpd.GeoDataFrame.from_file('data/SIG_20220324/lon_lat/lon_lat.shp', encoding='utf8'),
    gpd.GeoDataFrame.from_file('data/EMD_20220324/lon_lat/lon_lat.shp', encoding='utf8'),
    gpd.GeoDataFrame.from_file('data/LI_20220324/lon_lat/lon_lat.shp', encoding='utf8')
]

points = [Point(lon, lat)]
for p in points:
    data = {}
    for area in all_area:
        result = area[area.geometry.contains(p)]
        result_dict = result.drop(columns=['geometry']).to_dict('records')
        if not result_dict:
            continue
        print (result_dict)

