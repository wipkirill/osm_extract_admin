# OSM admin boundaries extractor demo project

This project is inspired by OSM community(see http://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative) and such tools as Mapzen Borders(https://mapzen.com/data/borders/), 
Postgis geometry simplification https://strk.kbt.io/blog/tag/postgis/ and many others. It is aimed to extract and
simplify borders of multiple administrative levels from OSM data. 



## Prerequisites
* Install Postgres from this link http://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS22UbuntuPGSQL95Apt
* Psycopg2 to connect from Python: 
```
sudo apt-get update
sudo apt-get install libpq-dev python-dev
sudo pip install psycopg2
```
* Install libosmium:
```
sudo apt-get install libosmium-dev libboost-dev libexpat1-dev libprotobuf-dev protobuf-compiler zlib1g-dev libxml2-dev
pip install -U osmium
```

## Running
* Extracting admin boundaries from raw PBF file(default 2-6 levels):
```
python extract_admin_pbf.py <file.pbf>
``` 
* Create Postgres database:
```
psql
CREATE DATABASE geodb
```
* Run main script to build heirarchy from osm data:
```
python build_areas.py --filename=planet-latest_osm_admin_2-6.osm.pbf --max_level=2  --psql="host=localhost dbname=geodb user=<user> password=<pswd>"
```

## Hardware limits

Main goal was to extract admin boundaries from all countries. This includes national borders, states, districts and subdistricts.
It was tested on a 20GB RAM node, 10 cores. 


