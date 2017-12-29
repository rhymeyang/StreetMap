sed -n '1,698 p'  org_shanghai_china.osm > shanghai_china.osm
sed -n '4611958,4612292 p' org_shanghai_china.osm >> shanghai_china.osm 
tail -n 47 org_shanghai_china.osm >> shanghai_china.osm 
