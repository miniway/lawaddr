import sys
import geopandas as gpd

infile = sys.argv[1]
outpath = sys.argv[2]
area = gpd.GeoDataFrame.from_file(infile, encoding='euc-kr')
print (area.crs)
area = area.to_crs(4326)
area.to_file(outpath, encoding='utf-8')

