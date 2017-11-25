#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os.path
import subprocess


# Arguments & help

ap = argparse.ArgumentParser(description='Process OSM administrative ' +
                             'boundaries as individual ways.')
ap.add_argument('-f', dest='min_admin_level', type=int, default=2,
                help='Minimum admin_level to retrieve.')
ap.add_argument('-t', dest='max_admin_level', type=int, default=6,
                help='Maximum admin_level to retrieve.')
ap.add_argument(dest='osm_input', metavar='planet.osm.pbf',
                help='An OpenStreetMap PBF file to process.')
args = ap.parse_args()


# Process & import admin boundaries with osmosis. Install http://wiki.openstreetmap.org/wiki/Osmosis

if args.min_admin_level == args.max_admin_level:
    admin_levels = args.min_admin_level;
    outfile = '{1}_osm_admin_{0}.osm.pbf'.format(admin_levels, args.osm_input.split(".")[0])
elif args.min_admin_level < args.max_admin_level:
    admin_levels = ','.join(str(i) for i in range(
                 args.min_admin_level, args.max_admin_level + 1))
    outfile = '{2}_osm_admin_{0}-{1}.osm.pbf'.format(
            args.min_admin_level, args.max_admin_level, args.osm_input.split(".")[0])
else:
    print('Error: max admin level cannot be be less than min admin level')
    exit(1)

if os.path.exists(outfile):
    print('Found existing file {0}; skipping filtering.'.format(outfile))
else:
    subprocess.call(['''osmosis/bin/osmosis \
        --read-pbf {0} \
        --tf accept-relations admin_level={1} \
        --tf accept-relations boundary=administrative \
        --used-way \
        --used-node \
        --write-pbf {2}'''.format(
            args.osm_input,
            admin_levels,
            outfile)],
        shell=True)
    print "Extracted admin only data to %s" % outfile

