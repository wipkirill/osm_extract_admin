#!/usr/bin/python -u
import osmium
import shapely.wkb as wkblib
from shapely import geos
import argparse
from collections import defaultdict

from serializer import Serializer, SimpleWriter
from geodb import Geodb

import logging

wkb_factory = osmium.geom.WKBFactory()

# http://gis.stackexchange.com/questions/48949/epsg-3857-or-4326-for-googlemaps-openstreetmap-and-leaflet
SRID = 4326
SRID_WKT = "SRID={srid};".format(srid=SRID)

def report_progress(obj):
    print({(tag.k, tag.v) for tag in obj.tags if tag.k in ['name', 'wikipedia']})


def extract_wkt(area):
    wkb = wkb_factory.create_multipolygon(area)
    return SRID_WKT + str(wkblib.loads(wkb, hex=True))


class FileStatsHandler(osmium.SimpleHandler):
    def __init__(self, tag_condition=None):
        super(FileStatsHandler, self).__init__()
        self._tag_condition = tag_condition
        self._added = set()
        self.nodes = 0
        self.ways = 0
        self.rels = 0
        self.areas = 0

    def node(self, n):
        self.nodes += 1

    def way(self, w):
        self.ways += 1

    def relation(self, r):
        self.rels += 1

    def area(self, a):
        obj_id = a.orig_id()
        if obj_id in self._added:
            logging.error('% id already added' % obj_id)
            return

        if self._tag_condition and not self._tag_condition({tag.k: tag.v for tag in a.tags}):
            logging.error('% ignored, doesnt satisfy condition' % obj_id)
            return
        id_to_props[obj_id] = {'tags': {(tag.k, tag.v) for tag in a.tags}}

        tags = {tag.k : tag.v for tag in a.tags}
        level_hint = tags['admin_level'] if 'admin_level' in tags and tags['admin_level'].isdigit() else None

        wkt = extract_wkt(a)
        geodb.insert(obj_id, wkt, level_hint)

        report_progress(a)
        self._added.add(obj_id)
        self.areas += 1

# http://wiki.openstreetmap.org/wiki/Tag:boundary%3Dmaritime
def baseline_border(tags):
    return tags["boundary"] == "maritime" and tags["border_type"] == "baseline"

def territorial_border(tags):
    return tags["boundary"] == "administrative" and tags["maritime"] == "yes" and tags["border_type"] == "territorial"

if __name__ == '__main__':
    geos.WKBWriter.defaults['include_srid'] = True

    parser = argparse.ArgumentParser(
        description='Build administrative areas polygons')
    parser.add_argument('--index_type',
                        dest='index_type',
                        required=False,
                        default="sparse_file_array",
                        choices=['sparse_mem_array', 'sparse_file_array',
                                 'dense_mem_array', 'dense_file_array'],
                        help='type of index type to store locations')
    parser.add_argument('--filename', dest='filename', required=True)
    parser.add_argument('--psql', default='host=localhost dbname=geodb user=postgres password=postgres', dest='psql',
                        required=True)
    parser.add_argument('--max_level', default=None, dest='max_level', required=True, type=int,
                        help='max admin level 0, 1, 2 ...')
    parser.add_argument('--threads', default=None, dest='threads', required=True, type=int,
                        help='number of threads for simplification')
    parser.add_argument('--border_table', default=None, dest='border_table', required=False,
                        help='border')
    parser.add_argument('--border_column', default=None, dest='border_column', required=False,
                        help='border column')
    args = parser.parse_args()

    geodb = Geodb(args.psql)

    id_to_props = defaultdict(lambda: defaultdict())
    handler = FileStatsHandler()
    handler.apply_file(args.filename, locations=True, idx=args.index_type)

    print("Nodes: %d" % handler.nodes)
    print("Ways: %d" % handler.ways)
    print("Relations: %d" % handler.rels)
    print("Areas: %d" % handler.areas)
    min_area = 1000000000
    try:
        geodb.build(max_overlap_ratio=0.8, min_area_ratio=1.5)
        if args.border_table and args.border_column:
            print "Cutting border..."
            geodb.cut_border(args.border_table, args.border_column, 0.01, min_area, args.max_level, threads=args.threads)
            print "Done cutting border"

        geodb.fix_custom()
        geodb.set_is_alone()
        geodb.fill_gaps()
        geodb.filter(max_level=args.max_level, area_in_meters_sqr=min_area)
        geodb.fix_levels()
        geodb.blow_up_geoms(0.001, min_area)

        #geodb.simplify_parallel_iterative(epsilon=[[0.2, 1.25], [0.0001, 4], [0.0001, 4]],
        #                                  max_points=[15000, 13000, 13000],
        #                                  max_level=args.max_level, threads=args.threads)
        writer = SimpleWriter('admin_areas_serialized')
        serializer = Serializer(writer)
        for geom_props in geodb.stream_topo_order(max_level=args.max_level):
            self_id, parent_id = geom_props['self_id'], geom_props['parent_id']
            geom_props.pop('self_id')
            geom_props.pop('parent_id')

            geom_props.update(id_to_props[self_id])
            serializer.write(self_id, parent_id, geom_props)

        #geodb.destroy()
    except Exception as e:
        logging.error(e)
        geodb.cancel_backend()
