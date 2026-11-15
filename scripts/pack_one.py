"""仅打包当前产品；逐片核对后返回体积和哈希，不累积全批中间瓦片。"""
from pathlib import Path
import hashlib,json,sys
from pmtiles.writer import write
from pmtiles.tile import zxy_to_tileid,Compression,TileType
from pmtiles.reader import MmapSource,all_tiles

folder=Path(sys.argv[1]);target=Path(sys.argv[2]);zoom=int(sys.argv[3])
provider={'IFS':'ECMWF','GFS':'NOAA'}[sys.argv[4] if len(sys.argv)>4 else 'IFS']
entries=[]
for path in (folder/'tiles').glob('*/*/*.mvt'):
    z,x,y=int(path.parent.parent.name),int(path.parent.name),int(path.stem)
    entries.append((zxy_to_tileid(z,x,y),path))
if not entries:
    raise ValueError('没有瓦片，不能发布未完成产品')
with write(str(target)) as writer:
    for tile_id,path in sorted(entries):
        writer.write_tile(tile_id,path.read_bytes())
    writer.finalize({'tile_type':TileType.MVT,'tile_compression':Compression.GZIP,
        'min_lon_e7':-1800000000,'min_lat_e7':-850511288,'max_lon_e7':1800000000,
        'max_lat_e7':850511288,'center_zoom':2,'center_lon_e7':0,'center_lat_e7':0},
        {'attribution':f'Open-Meteo / {provider}, CC BY 4.0; derived contours and isobands',
         'vector_layers':[{'id':'weather','fields':{'value':'Number','color':'String'},'minzoom':0,'maxzoom':zoom}]})
count=0
with target.open('rb') as stream:
    for (z,x,y),body in all_tiles(MmapSource(stream)):
        if body != (folder/f'tiles/{z}/{x}/{y}.mvt').read_bytes():
            raise ValueError('PMTiles 与输入瓦片不一致')
        count+=1
if count != len(entries):
    raise ValueError('PMTiles 片数不一致')
print(json.dumps({'bytes':target.stat().st_size,'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'verified_tiles':count}))
