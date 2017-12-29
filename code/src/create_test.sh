head -n 1000 org_shanghai_china.osm > shanghai_china.osm
sed -n '4611958,4613211 p' org_shanghai_china.osm >> shanghai_china.osm 
tail -n 47 org_shanghai_china.osm >> shanghai_china.osm 
