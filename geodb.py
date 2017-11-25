import os

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import sys

import subprocess
from psycopg2.pool import ThreadedConnectionPool

import argparse
from random import randint
from threading import RLock
from concurrent.futures import ThreadPoolExecutor, Future, TimeoutError

# Postgres routines and SQL

def new_psql_con(conn, psql, dbname):
    tokens = [t.split('=') for t in psql.split()]
    for t in tokens:
        if len(t) != 2:
            raise Exception('Not a valid psql connection', psql)
    if 'dbname' not in [t[0] for t in tokens]:
        raise Exception('dbname not present in', psql)
    try:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.cursor().execute('create database '+ dbname)
    except Exception, e:
        print e.pgerror
        raise
    finally:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    return psycopg2.connect(' '.join(['='.join([t[0], t[1] if t[0] != 'dbname' else dbname]) for t in tokens]))


class _ParallelFunctor(object):
    def __init__(self, conn_pool):
        self._conn_pool = conn_pool

    def func(self, *args, **kwargs):
        raise NotImplementedError('Must be implemented')

    def __call__(self, *args):
        conn = self._conn_pool.getconn()
        if hasattr(args[0], '__iter__'):
            self.func(conn, *(args[0]))
        else:
            self.func(conn, *args)

        self._conn_pool.putconn(conn)


def task_queue(task, iterator, concurrency=10):
    def submit():
        try:
            obj = next(iterator)
        except StopIteration:
            return
        if result.cancelled():
            return
        stats['delayed'] += 1
        future = executor.submit(task, obj)
        future.obj = obj
        future.add_done_callback(computation_done)

    def computation_done(future):
        with io_lock:
            submit()
            stats['delayed'] -= 1
            stats['done'] += 1
        if future.exception():
            raise future.exception()
        if stats['delayed'] == 0:
            result.set_result(stats)

    def cleanup(_):
        with io_lock:
            executor.shutdown(wait=False)

    io_lock = RLock()
    executor = ThreadPoolExecutor(concurrency)
    result = Future()
    result.stats = stats = {'done': 0, 'delayed': 0}
    result.add_done_callback(cleanup)

    with io_lock:
        for _ in range(concurrency):
            submit()

    return result

class Geodb(object):
    def __init__(self, psql_str):
        self._conn = psycopg2.connect(psql_str)
        self._psql_str = psql_str
        self._try_lock = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            SELECT 1;
        '''
        self._insert_sql = '''
            INSERT INTO geodb(self_id, geom, level_hint) VALUES (%s, %s, %s);
        '''
        self._get_all_sql = '''
            SELECT * FROM geodb;
        '''
        self._get_sql = '''
            SELECT * FROM geodb WHERE self_id = %s;
        '''
        self._get_by_level_sql = '''
            SELECT * FROM geodb WHERE level = %s;
        '''
        self._get_topo_order_sql = '''
            SELECT * FROM geodb ORDER BY level ASC;
        '''
        self._get_topo_order_max_level_sql = '''
            SELECT * FROM geodb WHERE level <= {max_level} ORDER BY level ASC;
        '''
        self._get_self_id_for_level_sql = '''
            SELECT self_id FROM (
                SELECT self_id FROM geodb WHERE level = {max_level} ORDER BY ST_Area(geography(geom))
            ) AS foo;
        '''
        self._has_levels_sql = '''
            SELECT COUNT(DISTINCT level) FROM geodb;
        '''
        self._get_levels_sql = '''
            SELECT DISTINCT level FROM geodb ORDER BY level;
        '''
        self._get_continents_sql = '''
            SELECT DISTINCT id FROM {continents_table} ORDER BY id;
        '''
        self._get_ordered_parents = '''
            SELECT parent_id FROM (
                SELECT parent_id, random() AS ordered FROM (
                    SELECT DISTINCT parent_id FROM geodb WHERE level = {level}
                ) AS foo ORDER BY ordered
            ) AS bar;
        '''
        self._sum_points_by_parent_id = '''
            SELECT SUM(ST_NPoints(simple_geom)) FROM {geodb_table} WHERE parent_id {ancestor_condition};
        '''
        self._create_extensions_sql = '''
            CREATE EXTENSION IF NOT EXISTS BTREE_GIST;
            CREATE EXTENSION IF NOT EXISTS POSTGIS;
            CREATE EXTENSION IF NOT EXISTS POSTGIS_TOPOLOGY;
            SET search_path = topology,public;
        '''
        self._create_table_sql = '''
            CREATE TABLE IF NOT EXISTS geodb(
                self_id BIGINT NOT NULL UNIQUE,
                parent_id BIGINT,
                level BIGINT,
                geom geometry NOT NULL,
                simple_geom geometry,
                level_hint BIGINT,
                is_alone BOOLEAN DEFAULT TRUE
            );
            CREATE TABLE IF NOT EXISTS geodb_disputed(
                self_id1 BIGINT NOT NULL,
                self_id2 BIGINT NOT NULL,
                geom geometry NOT NULL
            );
        '''
        self._drop_table_if_exists_sql = '''
            DROP TABLE IF EXISTS {name} CASCADE;
        '''
        self._create_table_border_sql = '''
            CREATE TABLE {border_name} as (SELECT * FROM {land_polygons_name} WHERE ST_AREA(geography(geom)) > {min_area});
        '''
        self._create_simple_border_table = '''
            CREATE TABLE {border_hash} as (SELECT * FROM {border_table} WHERE ST_AREA(geography(geom)) > {min_area});
            CREATE INDEX geom_idx_{border_hash} on {border_hash} using gist({border_column});
        '''
        self._simplify_border_table = '''
            UPDATE {border_hash} SET {border_column} = ST_Multi(ST_SimplifyPreserveTopology({border_column}, {epsilon}));
        '''
        self._simplify_buffer_border_table = '''
            UPDATE {border_hash} SET {border_column} = ST_Multi(ST_Buffer(ST_SimplifyPreserveTopology({border_column}, {epsilon}), {buffer}));
        '''
        self._set_is_alone_sql = '''
            UPDATE geodb AS current SET is_alone = FALSE
                WHERE EXISTS (
                    SELECT 1 FROM geodb AS neighbor
                    WHERE ST_Intersects(current.geom, neighbor.geom) AND current.self_id != neighbor.self_id
                    AND current.level=neighbor.level
                );
        '''
        self._fill_gaps_sql = '''
            UPDATE geodb SET geom = subquery.geom from
            (
                SELECT self_id, ST_Collect(ST_MakePolygon(geom)) AS geom FROM (select self_id, st_exteriorring(geom) AS geom FROM
                    (SELECT self_id, (ST_Dump(geom)).geom AS geom FROM geodb) AS b) AS c GROUP BY self_id)
            AS subquery WHERE subquery.self_id = geodb.self_id;
        '''
        self._filter_geometries = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION filter_geometries() RETURNS VOID AS $$
                DECLARE
                    record RECORD;
                    lvl BIGINT := 0;
                BEGIN
                    DELETE FROM geodb WHERE level > 3;
                    CREATE TEMP TABLE to_remove(self_id BIGINT);
                    INSERT INTO to_remove SELECT self_id FROM geodb
                        WHERE ST_AREA(geography(geom)) < {area_in_meters_sqr} AND level > 1 AND is_alone;
                    DELETE FROM geodb WHERE EXISTS (SELECT 1 FROM to_remove WHERE to_remove.self_id = geodb.self_id);
                    FOR record IN SELECT self_id FROM to_remove LOOP
                        RAISE INFO 'Filtering % because its too small', record.self_id;
                    END LOOP;
                    DROP TABLE to_remove CASCADE;

                    -- remove areas with non existing parent
                    CREATE TEMP TABLE to_remove AS (SELECT self_id from geodb as child WHERE NOT EXISTS
                        (SELECT 1 FROM geodb AS parent WHERE parent.self_id=child.parent_id) AND level>0);
                    DELETE FROM geodb WHERE EXISTS (SELECT 1 FROM to_remove WHERE to_remove.self_id = geodb.self_id);

                    DROP TABLE to_remove CASCADE;
                    LOOP
                        -- remove small polygons inside multipolygons of countries
                        UPDATE geodb SET geom = subquery.geom FROM (
                            SELECT self_id, ST_CollectionExtract(ST_Collect(geom), 3) AS geom FROM
                                (
                                    SELECT self_id, geom FROM
                                    (
                                        SELECT self_id, level, (ST_Dump(geom)).geom AS geom
                                        FROM geodb WHERE ST_AREA(geography(geom)) > {area_in_meters_sqr} AND level = lvl
                                    ) AS geodb_dump WHERE ST_AREA(geography(geom)) > {area_in_meters_sqr}
                                ) as geodb_filtered
                            GROUP BY self_id
                        ) AS subquery WHERE geodb.self_id = subquery.self_id AND subquery.geom IS NOT NULL;
                        RAISE INFO 'Filtered small parts of countries for level %', lvl;

                        lvl = lvl + 1;
                        EXIT WHEN lvl > {max_level};
                    END LOOP;
                END;
            $$ LANGUAGE plpgsql;
            SELECT filter_geometries();
        '''
        self._cancel_backend = '''
            SELECT pg_cancel_backend(pid) FROM (select pid, datname, usename, client_addr, query
                FROM pg_stat_activity WHERE query NOT LIKE '%pg_stat_activity%') AS pids;
        '''
        self._verify_srid = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION verify_srid() RETURNS VOID AS $$
                DECLARE
                    num_srids BIGINT;
                BEGIN
                    SELECT COUNT(DISTINCT ST_SRID(geom)) FROM geodb INTO num_srids;
                    IF num_srids != 1 THEN
                        RAISE EXCEPTION 'Geometries have different srids';
                    END IF;
                    SELECT 1 FROM geodb WHERE ST_SRID(geom) = 0 INTO num_srids;
                    IF num_srids != 0 THEN
                        RAISE EXCEPTION 'Geometries have NULL srid';
                    END IF;
                END;
            $$ LANGUAGE plpgsql;
            SELECT verify_srid();
        '''
        self._check_types = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION check_types() RETURNS VOID AS $$
                DECLARE
                    record RECORD;
                BEGIN
                    FOR record IN SELECT ST_GeometryType(geom) as type, COUNT(*) as count FROM geodb GROUP BY ST_GeometryType(geom) LOOP
                        RAISE INFO 'Number of type % - %', record.type, record.count;
                    END LOOP;
                END;
            $$ LANGUAGE plpgsql;
            SELECT check_types();
        '''
        self._remove_duplicate_geoms = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION remove_duplicates() RETURNS VOID AS $$
                DECLARE
                    duplicates BIGINT = 0;
                    record RECORD;
                BEGIN
                    RAISE INFO 'Finding duplicates %', timeofday();
                    CREATE INDEX tmp_duplicate_geoms_idx ON geodb USING GIST(geom, self_id);
                    CREATE TEMP TABLE toremove(self_id BIGINT NOT NULL);
                    FOR record IN SELECT a.self_id AS x, b.self_id AS y FROM geodb AS a, geodb AS b
                        WHERE ST_Equals(a.geom, b.geom) AND a.self_id < b.self_id LOOP
                        RAISE INFO '% and % have same geometry. Removing %', record.x, record.y, record.x;
                        EXECUTE 'INSERT INTO toremove VALUES($1)' USING record.x;
                        duplicates = duplicates + 1;
                    END LOOP;
                    CREATE INDEX toremove_id_idx ON toremove(self_id);

                    RAISE INFO 'Number of duplicates %', duplicates;
                    DELETE FROM geodb
                        WHERE EXISTS (SELECT 1 FROM toremove WHERE geodb.self_id = toremove.self_id);
                    DROP INDEX tmp_duplicate_geoms_idx;
                    DROP TABLE toremove CASCADE;
                    RAISE INFO 'Finished finding duplicates %', timeofday();
                END;
            $$ LANGUAGE plpgsql;
            SELECT remove_duplicates();
        '''
        self._check_geoms_valid_and_simple = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION check_geoms_valid_and_simple() RETURNS VOID AS $$
                DECLARE
                    record RECORD;
                BEGIN
                    RAISE INFO 'Making geoms valid and simple %', timeofday();
                    FOR record IN SELECT * FROM geodb WHERE NOT ST_IsValid(geom) OR NOT ST_IsSimple(geom) LOOP
                        RAISE EXCEPTION '% is not simple or not valid', record.self_id;
                    END LOOP;
                    --UPDATE geodb SET geom=ST_RemoveRepeatedPoints(geom, {epsilon});
                    -- Show empty geometries
                    FOR record IN SELECT * FROM geodb WHERE ST_IsEmpty(geom) LOOP
                        RAISE EXCEPTION '% has empty geometry', record.self_id;
                    END LOOP;
                    --Remove empty geometries
                    DELETE FROM geodb WHERE ST_IsEmpty(geom);
                    RAISE INFO 'Finished making geoms valid and simple %', timeofday();
                END;
            $$ LANGUAGE plpgsql;
            SELECT check_geoms_valid_and_simple();
        '''
        self._find_levels = '''
            -- largest - level 0, smallest - large level
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION find_levels() RETURNS VOID AS $$
                DECLARE
                    level BIGINT := 0;
                    toremove_size BIGINT := 0;
                    geodb_size BIGINT := 0;
                    geodb_size_after BIGINT := 0;
                    queue_size BIGINT := 0;
                    with_no_level BIGINT := 0;
                    record RECORD;
                BEGIN
                    RAISE INFO 'Finding levels %', timeofday();
                    SELECT COUNT(*) FROM geodb INTO geodb_size;
                    RAISE INFO 'Geodb size %', geodb_size;

                    CREATE TEMP TABLE queue AS (SELECT self_id, geom FROM geodb);
                    CREATE INDEX queue_geom_idx ON queue USING GIST(geom, self_id);
                    CREATE UNIQUE INDEX queue_id_idx ON queue(self_id);

                    SELECT COUNT(*) FROM queue INTO queue_size;
                    RAISE INFO 'Queue size %', queue_size;

                    LOOP
                        -- select those that do not have a parent
                        CREATE TEMP TABLE toremove(self_id BIGINT NOT NULL UNIQUE);
                        INSERT INTO toremove
                             SELECT parent.self_id FROM queue AS parent
                             WHERE
                                 NOT EXISTS(
                                 SELECT 1 FROM queue AS child
                                     WHERE ST_Contains(child.geom, parent.geom)
                                     AND parent.self_id != child.self_id);

                        SELECT COUNT(*) FROM toremove INTO toremove_size;
                        RAISE INFO 'Removing %', toremove_size;

                        DELETE FROM queue
                            WHERE EXISTS (SELECT 1 FROM toremove WHERE toremove.self_id = queue.self_id);
                        queue_size = queue_size - toremove_size;

                        EXECUTE 'UPDATE geodb SET level = $1
                            FROM toremove WHERE geodb.self_id = toremove.self_id' USING level;

                        IF queue_size > 0 AND toremove_size = 0 THEN
                            FOR record IN SELECT * FROM queue LOOP
                                RAISE INFO 'Left in queue %', record.self_id;
                            END LOOP;
                            RAISE EXCEPTION 'Queue not empty but nothing to remove. There are duplicate geometries.';
                        END IF;

                        EXIT WHEN NOT EXISTS (SELECT 1 FROM queue);

                        level = level + 1;
                        DROP TABLE toremove CASCADE;
                    END LOOP;

                    SELECT COUNT(*) FROM geodb WHERE geodb.level IS NULL INTO with_no_level;
                    RAISE INFO 'Left with no level %', with_no_level;

                    SELECT COUNT(*) FROM geodb INTO geodb_size_after;
                    RAISE INFO 'Geodb size after %', geodb_size_after;

                    FOR record IN SELECT DISTINCT geodb.level FROM geodb ORDER BY geodb.level LOOP
                        RAISE INFO 'Found level %', record.level;
                    END LOOP;

                    CREATE INDEX IF NOT EXISTS geodb_level_idx ON geodb(level);
                    CREATE INDEX IF NOT EXISTS geodb_parent_idx ON geodb(parent_id);
                    RAISE INFO 'Finished finding levels %', timeofday();
                END;
            $$ LANGUAGE plpgsql;
            SELECT find_levels();
        '''
        self._find_parents = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION find_parents() RETURNS VOID AS $$
                DECLARE
                    level BIGINT := 0;
                    slice_size BIGINT := 0;
                    found_size BIGINT := 0;
                BEGIN
                    RAISE INFO 'Finding parents %', timeofday();
                    SELECT MAX(geodb.level) FROM geodb INTO level;
                    LOOP
                        RAISE INFO 'Finding parent level %', level;
                        CREATE TEMP TABLE slice(
                            self_id BIGINT NOT NULL UNIQUE,
                            geom geometry NOT NULL
                        );
                        EXECUTE 'INSERT INTO slice SELECT self_id, geom
                            FROM geodb WHERE $1-1 <= level AND level <= $1' USING level;
                        CREATE INDEX slice_geom_idx ON slice USING GIST(geom, self_id);

                        SELECT COUNT(*) FROM slice INTO slice_size;
                        RAISE INFO 'Slice size %', slice_size;
                        EXIT WHEN slice_size = 0;

                        UPDATE geodb SET parent_id=subquery.parent_id FROM (
                            SELECT parent.self_id AS parent_id, child.self_id AS self_id
                                FROM slice AS child, slice AS parent
                                WHERE child.self_id != parent.self_id
                                    AND ST_Contains(parent.geom, child.geom)) AS subquery
                            WHERE subquery.self_id = geodb.self_id;
                        level = level - 1;
                        DROP TABLE slice CASCADE;
                    END LOOP;
                    RAISE INFO 'Finished finding parents %', timeofday();
                END;
            $$ LANGUAGE plpgsql;
            SELECT find_parents();
        '''
        self._resolve_wrong_level = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION resolve_wrong_level() RETURNS VOID AS $$
                BEGIN
                    RAISE INFO 'Resolving wrong level %', timeofday();
                    UPDATE geodb SET level = subquery.level, parent_id = subquery.parent_id FROM (
                        SELECT wrong_level.self_id, maybe_parent.self_id as parent_id, maybe_parent.level + 1 as level FROM geodb AS wrong_level, geodb AS maybe_parent
                            WHERE wrong_level.self_id != maybe_parent.self_id AND
                                  wrong_level.level = maybe_parent.level AND
                                  ST_Intersects(wrong_level.geom, maybe_parent.geom) AND
                                  ST_Area(geography(wrong_level.geom)) * {max_overlap_ratio} < ST_Area(geography(ST_Intersection(maybe_parent.geom, wrong_level.geom))) AND
                                  ST_Area(geography(wrong_level.geom)) * {min_area_ratio} < ST_Area(geography(maybe_parent.geom))
                    ) AS subquery WHERE subquery.self_id = geodb.self_id;
                    RAISE INFO 'Finished resolving wrong level %', timeofday();
                END;
            $$ LANGUAGE plpgsql;
            SELECT resolve_wrong_level();
        '''
        self.fix_levels_sql = '''
            CREATE OR REPLACE FUNCTION fix_level(parent BIGINT, current_level BIGINT) RETURNS VOID AS $$
                DECLARE
                    record RECORD;
                BEGIN
                    IF current_level > 2 THEN
                        RETURN;
                    END IF;
                    UPDATE geodb SET level = current_level + 1 where parent_id = parent;
                    FOR record IN SELECT self_id, level FROM geodb WHERE parent_id = parent LOOP
                        PERFORM fix_level(record.self_id, record.level);
                    END LOOP;
                END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION fix_levels() RETURNS VOID AS $$
                DECLARE
                    record RECORD;
                BEGIN
                    FOR record IN SELECT self_id, level FROM geodb WHERE level = 0 LOOP
                        RAISE INFO 'Fixing levels for country %', record.self_id;
                        PERFORM fix_level(record.self_id, record.level);
                    END LOOP;
                END;
            $$ LANGUAGE plpgsql;
            SELECT fix_levels();
        '''
        self._resolve_disputed = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION resolve_disputed() RETURNS VOID AS $$
                DECLARE
                    record RECORD;
                BEGIN
                    DELETE FROM geodb_disputed;
                    -- insert pairs of countries that have disputed territories
                    INSERT INTO geodb_disputed
                        SELECT geom1.self_id, geom2.self_id, ST_Multi(ST_CollectionExtract(ST_Intersection(geom1.geom, geom2.geom), 3))
                        FROM geodb AS geom1, geodb AS geom2
                        WHERE geom1.self_id != geom2.self_id AND ST_Intersects(geom1.geom, geom2.geom) AND
                            geom1.level = 0 AND geom2.level = 0 AND
                            NOT ST_IsEmpty(ST_CollectionExtract(ST_Intersection(geom1.geom, geom2.geom), 3));

                END;
            $$ LANGUAGE plpgsql;
            SELECT resolve_disputed();
        '''
        self._get_disputed = '''
            SELECT DISTINCT(geodb.self_id) AS terr_id, disp.self_id1 AS country1, disp.self_id2 AS country2, geodb.level AS lvl
            FROM geodb, geodb_disputed
            AS disp WHERE ST_Intersects(geodb.geom, disp.geom) AND geodb.self_id != disp.self_id1 AND geodb.self_id != disp.self_id2
            AND geodb.level < 3 AND disp.self_id1 < disp.self_id2
            ORDER BY disp.self_id1, disp.self_id2;
        '''
        self._verify_parents = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION find_parents() RETURNS VOID AS $$
                DECLARE
                    level BIGINT := 0;
                    record RECORD;
                BEGIN
                    FOR record IN SELECT * FROM geodb AS child WHERE
                       EXISTS (SELECT 1 FROM geodb AS parent
                         WHERE parent.self_id = child.parent_id
                            AND parent.level > child.level) LOOP
                        RAISE EXCEPTION '% has level % smaller than parent %', record.self_id, record.level, record.parent_id;
                    END LOOP;
                END;
            $$ LANGUAGE plpgsql;
            SELECT find_parents();
        '''
        self._drop_topologies = '''
            SELECT topology.DropTopology('geodb_topo_{hash}');
        '''
        self._create_geodb_per_parent = '''
            CREATE TABLE geodb_{hash} AS SELECT self_id, parent_id, level, geom, simple_geom
                FROM geodb WHERE parent_id {ancestor_condition};
        '''
        self._remove_from_geodb_per_parent = '''
            CREATE OR REPLACE FUNCTION remove_from_geodb_per_parent_{hash}() RETURNS VOID AS $$
                DECLARE
                    geodb_count BIGINT := 0;
                BEGIN
                    SELECT COUNT(*) FROM geodb_{hash} INTO geodb_count;
                    RAISE INFO 'Geodb size before % cont_id={continent_id}', geodb_count;
                    CREATE TEMP TABLE to_remove_{hash}(self_id BIGINT);
                    INSERT INTO to_remove_{hash} SELECT self_id FROM geodb_{hash}, {continents_table}
                        WHERE NOT ST_Intersects({continents_table}.geom, geodb_{hash}.geom) AND {continents_table}.id={continent_id};
                    DELETE FROM geodb_{hash} WHERE EXISTS (SELECT 1 FROM to_remove_{hash} WHERE to_remove_{hash}.self_id = geodb_{hash}.self_id);
                    SELECT COUNT(*) FROM geodb_{hash} INTO geodb_count;
                    RAISE INFO 'Geodb size after % for cont_id={continent_id}', geodb_count;
                    DROP TABLE to_remove_{hash};
                END;
            $$ LANGUAGE plpgsql;
            SELECT remove_from_geodb_per_parent_{hash}();
        '''
        self._drop_geodb_per_parent = '''
            DROP TABLE IF EXISTS geodb_{hash} CASCADE;
            DROP TABLE IF EXISTS simple_geodb_{hash} CASCADE;
        '''
        self._setup_simplify_polygons = '''
            CREATE OR REPLACE FUNCTION setup_simplify_polygons_{hash}() RETURNS VOID AS $$
                DECLARE
                    srid INTEGER;
                    simple_geodb_rows BIGINT;
                    record RECORD;
                BEGIN
                    SELECT ST_SRID(geom) FROM geodb LIMIT 1 INTO srid;
                    IF srid = 0 THEN
                        RAISE EXCEPTION 'Zero srid found';
                    END IF;

                    PERFORM topology.CreateTopology('geodb_topo_{hash}', srid, 0);

                    CREATE UNLOGGED TABLE public.simple_geodb_{hash} AS (SELECT self_id AS old_id, (ST_Dump(geom)).geom AS geom, NULL::geometry AS simple_geom
                                                                         FROM {geodb_table});
                    SELECT COUNT(*) FROM simple_geodb_{hash} INTO simple_geodb_rows;
                    RAISE INFO 'simple_geodb has % records', simple_geodb_rows;

                    ALTER TABLE simple_geodb_{hash} ADD COLUMN self_id BIGSERIAL PRIMARY KEY;
                    CREATE INDEX simple_geodb_{hash}_geom_idx ON simple_geodb_{hash} USING GIST(geom, self_id);

                    RAISE INFO 'Simplifying geometries for {level} %', timeofday();
                    PERFORM topology.AddTopoGeometryColumn('geodb_topo_{hash}', 'public', 'simple_geodb_{hash}', 'topo_col', 'MULTIPOLYGON') AS new_layer_id;
                    RAISE INFO 'topo_col = toTopoGeom for {level} %', timeofday();

                    --LOCK TABLE geodb IN EXCLUSIVE MODE;
                    FOR record IN SELECT old_id FROM simple_geodb_{hash}
                    LOOP
                        BEGIN
                            RAISE WARNING 'Adding % to topology', record.old_id;
                            UPDATE simple_geodb_{hash} SET topo_col = toTopoGeom(geom, 'geodb_topo_{hash}', 1) WHERE old_id=record.old_id;
                        EXCEPTION
                             WHEN OTHERS THEN
                                 RAISE WARNING 'Failed to add into topology %', record.old_id;
                        END;
                    END LOOP;

                    RAISE WARNING 'Finished topo_col = toTopoGeom for {level} %', timeofday();
                END;
            $$ LANGUAGE plpgsql;
            SELECT setup_simplify_polygons_{hash}();
        '''
        self._update_simple_geom = '''
            UPDATE geodb SET simple_geom = geom;
        '''
        self._clear_simple_geom_at_level = '''
            UPDATE geodb SET simple_geom = NULL WHERE level={level};
        '''
        self._simplify_polygons = '''
            CREATE OR REPLACE FUNCTION simplify_edge_geom_{hash}(topology_param text,
                edge_id_param integer, tolerance_param double precision,
                num_iterations_param integer DEFAULT 5) RETURNS double precision AS $$
                DECLARE
                     i integer := 1;
                     func_suffix text := '';
                     tolerance_var double precision := tolerance_param;
                     sql_var text;
                BEGIN
                     WHILE (i <= num_iterations_param) LOOP
                         sql_var := 'SELECT topology.ST_ChangeEdgeGeom(' || quote_literal(topology_param) || ', ' || edge_id_param
                           || ', ST_Simplify' || func_suffix || '(geom, ' || tolerance_var || ')) FROM '
                           || quote_ident(topology_param) || '.edge WHERE edge_id = ' || edge_id_param;
                         BEGIN
                             RAISE DEBUG 'Running %', sql_var;
                             EXECUTE sql_var;
                             RETURN tolerance_var;
                         EXCEPTION
                             WHEN OTHERS THEN
                                 RAISE WARNING 'Simplification of edge % failed in iteration %_%, with tolerance %: %',
                                     edge_id_param, i, func_suffix, tolerance_var, SQLERRM;

                                 IF (position('not simple' IN SQLERRM) > 0) AND (func_suffix = '') THEN
                                     func_suffix := 'PreserveTopology';
                                 ELSE
                                     i := i + 1;
                                     func_suffix := '';
                                     tolerance_var := ROUND( (tolerance_var/4.0) * 1e8 ) / 1e8;
                                 END IF;
                             END;
                         -- END EXCEPTION
                     END LOOP;
                     RETURN NULL;
                END;
            $$ LANGUAGE 'plpgsql' STABLE STRICT;

            CREATE OR REPLACE FUNCTION simplify_polygons_{hash}() RETURNS VOID AS $$
                DECLARE
                    simple_geodb_rows BIGINT;
                BEGIN
                    RAISE INFO 'simplify_edge_geom for {level} %', timeofday();
                    PERFORM simplify_edge_geom_{hash}('geodb_topo_{hash}', edge_id, {epsilon}) FROM geodb_topo_{hash}.edge;
                    RAISE INFO 'Finished simplify_edge_geom for {level} %', timeofday();

                    UPDATE simple_geodb_{hash} SET simple_geom = topo_col::geometry;

                    SELECT COUNT(*) FROM simple_geodb_{hash} WHERE simple_geom IS NULL INTO simple_geodb_rows;
                    RAISE INFO 'simple_geodb_{hash} has % records with NULL simple_geom', simple_geodb_rows;

                    RAISE INFO 'UPDATE {geodb_table} SET simple_geom = for {level} %', timeofday();
                    UPDATE {geodb_table} SET simple_geom = subquery.simple_geom FROM (
                            SELECT simple_geodb_{hash}.old_id AS self_id, ST_CollectionExtract(ST_Collect(simple_geodb_{hash}.simple_geom), 3) AS simple_geom
                            FROM simple_geodb_{hash}
                            GROUP BY simple_geodb_{hash}.old_id) AS subquery
                        WHERE {geodb_table}.self_id = subquery.self_id;
                    RAISE INFO 'Done UPDATE {geodb_table} SET simple_geom = for {level} %', timeofday();

                    RAISE INFO 'Finished simplifying geometries %', timeofday();
                EXCEPTION WHEN OTHERS THEN
                    DROP TABLE simple_geodb_{hash} CASCADE;
                    RAISE WARNING 'Could not simplify due to exception %', SQLERRM;
                END;
            $$ LANGUAGE plpgsql;
            SELECT simplify_polygons_{hash}();
            DROP FUNCTION simplify_polygons_{hash}();
            DROP FUNCTION simplify_edge_geom_{hash}(text, integer, double precision, integer);
        '''
        self._blow_up_geoms = '''
            CREATE OR REPLACE FUNCTION blow_up_geoms_{self_id}() RETURNS VOID AS $$
                DECLARE
                    record RECORD;
                BEGIN
                    UPDATE geodb SET geom = ST_Buffer(geom, {epsilon}, 'quad_segs=4') WHERE self_id = {self_id};
                    FOR record IN SELECT neighbor.geom FROM geodb AS neighbor, geodb AS current
                        WHERE neighbor.level = current.level AND neighbor.self_id != current.self_id AND
                              current.self_id = {self_id} AND ST_Intersects(neighbor.geom, current.geom) LOOP
                        UPDATE geodb SET geom = ST_CollectionExtract(ST_Difference(geodb.geom, record.geom), 3) WHERE geodb.self_id = {self_id};
                    END LOOP;
                    UPDATE geodb SET geom = subquery.geom FROM (
                        SELECT self_id, ST_CollectionExtract(ST_Collect(geom), 3) AS geom FROM
                            (
                                SELECT self_id, geom FROM
                                (
                                    SELECT self_id, (ST_Dump(geom)).geom AS geom FROM geodb WHERE self_id = {self_id}
                                ) AS geodb_dump WHERE ST_AREA(geography(geom)) > {area_in_meters_sqr}
                            ) as geodb_filtered
                        GROUP BY self_id
                    ) AS subquery WHERE geodb.self_id = subquery.self_id AND subquery.geom IS NOT NULL;
                END;
            $$ LANGUAGE plpgsql;
            SELECT blow_up_geoms_{self_id}();
            DROP FUNCTION blow_up_geoms_{self_id}();
        '''
        self._copy_simplified_geoms = '''
            UPDATE geodb SET simple_geom = geodb_{hash}.simple_geom FROM geodb_{hash} WHERE geodb.self_id = geodb_{hash}.self_id;
        '''
        self._union_simplified_geoms = '''
            UPDATE geodb SET simple_geom = geodb_{hash}.simple_geom
                FROM geodb_{hash} WHERE geodb.self_id = geodb_{hash}.self_id AND geodb.simple_geom IS NULL;
            UPDATE geodb SET simple_geom = ST_CollectionExtract(ST_UNION(geodb.simple_geom, geodb_{hash}.simple_geom), 3)
                FROM geodb_{hash} WHERE geodb.self_id = geodb_{hash}.self_id AND geodb.simple_geom IS NOT NULL;
        '''
        self._verify_simplification = '''
            LOCK TABLE geodb IN EXCLUSIVE MODE;
            CREATE OR REPLACE FUNCTION verify_simplification() RETURNS VOID AS $$
                DECLARE
                    record RECORD;
                    no_simplified_count BIGINT;
                    total_count BIGINT;
                BEGIN
                    FOR record IN SELECT * FROM geodb WHERE simple_geom IS NULL AND level = {level} LOOP
                        RAISE INFO 'No simplified geometry for %', record.self_id;
                    END LOOP;
                    SELECT COUNT(*) FROM geodb WHERE simple_geom IS NULL AND level = {level} INTO no_simplified_count;
                    RAISE INFO 'Total not simplified %', no_simplified_count;
                    SELECT COUNT(*) FROM geodb WHERE level = {level} INTO total_count;
                    RAISE INFO 'Out of total %', total_count;
                END;
            $$ LANGUAGE plpgsql;
            SELECT verify_simplification();
        '''
        self._cut_border_with_self_id_sql = '''
            CREATE OR REPLACE FUNCTION cut_border_{hash}() RETURNS VOID AS $$
                BEGIN
                    RAISE INFO 'Cutting with border {self_id}';
                    CREATE UNLOGGED TABLE geodb_copy_{hash} AS SELECT * FROM geodb WHERE self_id={self_id};
                    CREATE INDEX IF NOT EXISTS geodb_copy_idx_{hash} ON geodb_copy_{hash} USING GIST(geom, self_id);
                    UPDATE geodb SET geom = subquery.geom FROM (
                        SELECT self_id, ST_CollectionExtract(ST_UNION(geom), 3) AS geom FROM
                            (
                             SELECT self_id, ST_Intersection(geodb_copy_{hash}.geom, {border_table}.{border_geom_column}) AS geom
                             FROM {border_table}, geodb_copy_{hash}
                             WHERE ST_Intersects(geodb_copy_{hash}.geom, {border_table}.{border_geom_column})
                            ) AS geodb_intersection
                            WHERE NOT ST_IsEmpty(geom) GROUP BY self_id
                    ) AS subquery WHERE geodb.self_id = subquery.self_id;
                    RAISE INFO 'Finished cutting with border {self_id}';
                    DROP table geodb_copy_{hash} CASCADE;
                END;
            $$ LANGUAGE 'plpgsql';
            SELECT cut_border_{hash}();
        '''
        self._cut_childs_with_parent_sql = '''
            CREATE OR REPLACE FUNCTION cut_childs_with_parent_{hash}() RETURNS VOID AS $$
                DECLARE
                    topo_order BIGINT := 0;
                    max_topo_order BIGINT := 0;
                BEGIN
                    RAISE INFO 'Cutting by ancestor {ancestor_id}';
                    UPDATE geodb SET geom = subquery.geom FROM (
                        SELECT self_id, ST_CollectionExtract(ST_UNION(geom), 3) AS geom FROM
                            (
                            SELECT child.self_id, ST_Intersection(parent.geom, child.geom) AS geom
                            FROM geodb AS parent, geodb AS child
                            WHERE child.parent_id = parent.self_id AND parent.self_id = {ancestor_id}
                            ) AS geodb_intersection
                        WHERE NOT ST_IsEmpty(geom) GROUP BY self_id
                    ) AS subquery WHERE geodb.self_id = subquery.self_id;
                    RAISE INFO 'Finished cutting by ancestor {ancestor_id}';
                END;
            $$ LANGUAGE 'plpgsql';
            SELECT cut_childs_with_parent_{hash}();
        '''
        self._custom_fix_countries_sql = '''
            -- Portugal has too much polygons in total ~900. Two of them has the most parts(islands). Their area in total
            -- exceeds {area_in_meters_sqr} but objects are very small. We cannot remove them entirely and
            -- take top 10 by area. It decreases num of polygons to 40 for Portugal

            UPDATE geodb SET geom = subquery.geom FROM (
                SELECT self_id, ST_CollectionExtract(ST_Collect(geom), 3) AS geom FROM
                    (
                        SELECT self_id, geom, st_area(geography(geom)) as area FROM
                        (
                            SELECT self_id, (ST_Dump(geom)).geom AS geom FROM geodb WHERE self_id=1629145
                        ) AS geodb_dump order by area desc limit 10
                    ) as geodb_filtered
                GROUP BY self_id
            ) AS subquery WHERE geodb.self_id = 1629145 AND subquery.geom IS NOT NULL;

            UPDATE geodb SET geom = subquery.geom FROM (
                SELECT self_id, ST_CollectionExtract(ST_Collect(geom), 3) AS geom FROM
                    (
                        SELECT self_id, geom, st_area(geography(geom)) as area FROM
                        (
                            SELECT self_id, (ST_Dump(geom)).geom AS geom FROM geodb WHERE self_id=1629146
                        ) AS geodb_dump order by area desc limit 10
                    ) as geodb_filtered
                GROUP BY self_id
            ) AS subquery WHERE geodb.self_id = 1629146 AND subquery.geom IS NOT NULL;
        '''
        with self._conn.cursor() as cur:
            cur.execute(self._create_extensions_sql)
            cur.execute(self._create_table_sql)

    def insert(self, self_id, wkt, level_hint):
        with self._conn.cursor() as cur:
            cur.execute(self._insert_sql, (self_id, wkt, level_hint))

    def _exec(self, conn, cur, sql):
        try:
            cur.execute(sql)
        finally:
            while conn.notices:
                print conn.notices.pop(0)

        conn.commit()

    def _exec_parallel(self, subtasks, functor, threads):
        conn_pool = ThreadedConnectionPool(threads, threads, self._psql_str)
        result = task_queue(functor(conn_pool), subtasks, threads)
        try:
            while not result.done():
                try:
                    result.result(.2)
                except TimeoutError:
                    pass
            print "done {done}, in work: {delayed}  ".format(**result.stats)
            sys.stdout.flush()
        except KeyboardInterrupt:
            result.cancel()
            raise

    def get_disputed(self):
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self._resolve_disputed)
        disputed = []
        for rec in self.stream_disputed():
            disputed.append(rec)

        return disputed


    def fix_levels(self):
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self.fix_levels_sql)

    def blow_up_geoms(self, epsilon, area_in_meters_sqr):
        #for level in self.get_levels(with_cursor=False):
        for self_id in [167454, 2171347, 51477, 1428125]:
            with self._conn.cursor() as cur:
                self._exec(self._conn, cur, self._blow_up_geoms.format(
                    epsilon=epsilon, area_in_meters_sqr=area_in_meters_sqr, self_id=self_id))

    def build(self, max_overlap_ratio, min_area_ratio):
        assert (max_overlap_ratio <= 1.0)
        assert (min_area_ratio >= 1.0)
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self._create_extensions_sql)
            self._exec(self._conn, cur, self._create_table_sql)
            self._exec(self._conn, cur, self._check_types)
            self._exec(self._conn, cur, self._verify_srid)
            self._exec(self._conn, cur, self._check_geoms_valid_and_simple.format(epsilon=1e-6))
            self._exec(self._conn, cur, self._remove_duplicate_geoms)
            self._exec(self._conn, cur, self._find_levels)
            self._exec(self._conn, cur, self._find_parents)
            self._exec(self._conn, cur, self._verify_parents)
            self._exec(self._conn, cur, self._resolve_wrong_level.format(
                max_overlap_ratio=max_overlap_ratio, min_area_ratio=min_area_ratio))

    def fill_gaps(self):
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self._fill_gaps_sql)

    def filter(self,  max_level, area_in_meters_sqr=0.0):
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self._filter_geometries.format(
                max_level=max_level, area_in_meters_sqr=float(area_in_meters_sqr)))

    def set_is_alone(self):
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self._set_is_alone_sql)

    def cut_border(self, border_table, border_geom_column, epsilon, min_area, max_level, threads):
        with self._conn.cursor() as cur:
            # make border table for level0
            border_hash = ("border_" + str(randint(0, long(1e18)))).lower()
            self._exec(self._conn, cur, self._create_simple_border_table.format(border_hash=border_hash,
                                                                                border_table=border_table,
                                                                                border_column=border_geom_column,
                                                                                min_area=min_area))
            self._exec(self._conn, cur, self._simplify_buffer_border_table.format(border_hash=border_hash,
                                                                                  border_column=border_geom_column,
                                                                                  epsilon=0.15,
                                                                                  buffer=0.05))
            # cut level0 with this border
            self._cut_border_parallel(0, border_hash, border_geom_column, threads)
            # make more detailed border for lvl1-2
            border_detailed_hash = ("border_" + str(randint(0, long(1e18)))).lower()
            self._exec(self._conn, cur, self._create_simple_border_table.format(border_hash=border_detailed_hash,
                                                                                border_table=border_table,
                                                                                border_column=border_geom_column,
                                                                                min_area=min_area))
            self._exec(self._conn, cur, self._simplify_border_table.format(border_hash=border_detailed_hash,
                                                                           epsilon=epsilon,
                                                                           border_column=border_geom_column))
            # cut level1 with this border
            self._cut_border_parallel(1, border_detailed_hash, border_geom_column, threads)
            self._cut_children_parallel(1, max_level, threads)
            # drop temp borders
            self._exec(self._conn, cur, self._drop_table_if_exists_sql.format(name=border_hash))
            self._exec(self._conn, cur, self._drop_table_if_exists_sql.format(name=border_detailed_hash))

    def cut(self, border_table, border_geom_column, max_level, threads):
        self._cut_border_parallel(border_table, border_geom_column, threads)
        self._cut_children_parallel(max_level, threads)

    def _cut_border_parallel(self, level, border_table, border_geom_column, threads):
        def cut_border(conn, self_id):
            with conn.cursor() as cur:
                hash = (str(self_id) + "_" + str(randint(0, long(1e18)))).lower()
                self._exec(conn, cur, self._cut_border_with_self_id_sql.format(self_id=self_id,
                                                                               border_table=border_table,
                                                                               border_geom_column=border_geom_column,
                                                                               hash=hash))

        class Functor(_ParallelFunctor):
            def func(self, *args):
                cut_border(*args)

        def subtasks():
            for self_id in self.stream_ordered_ids(level, with_cursor=False):
                yield self_id

        self._exec_parallel(subtasks(), Functor, threads)

    def _cut_children_parallel(self, start_level, max_level, threads):
        def cut_childs_with_parent(conn, ancestor_id):
            with conn.cursor() as cur:
                hash = (str(ancestor_id) + "_" + str(randint(0, long(1e18)))).lower()
                self._exec(conn, cur, self._cut_childs_with_parent_sql.format(ancestor_id=ancestor_id,
                                                                              hash=hash))

        class Functor(_ParallelFunctor):
             def func(self, *args):
                cut_childs_with_parent(*args)

        def subtasks():
            for level in self.get_levels(with_cursor=False):
                if start_level <= level < max_level:
                    for ancestor_id in self.stream_ordered_ids(level, with_cursor=False):
                        yield ancestor_id

        self._exec_parallel(subtasks(), Functor, threads)

    def simplify_parallel_iterative(self, epsilon, max_points, max_level, threads):
        continents_table = "continents"
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self._update_simple_geom)

        def simplify_level_parent(conn, level, ancestor_id, continent_id=None):
            with conn.cursor() as cur:
                try:
                    if not ancestor_id:
                        assert continent_id is not None, "Continent id is none"
                        ancestor_condition = "IS NULL"
                    else:
                        ancestor_condition = " = " + str(ancestor_id)
                    total_points = self._points_by_id(conn, cur, 'geodb', ancestor_condition)
                    print "Total points before simplification %d for %s level=%d" % (total_points, ancestor_condition, level)
                    if total_points <= max_points[level]:
                        print "No simplification run for %s" % ancestor_condition
                        return

                    hash = (str(level) + "_" + str(ancestor_id) + "_" + str(randint(0, long(1e18)))).lower()
                    self._exec(conn, cur, self._create_geodb_per_parent.format(parent_id=ancestor_id, hash=hash,
                                                                               ancestor_condition=ancestor_condition))
                    # Removes geoms not intersecting current continent_id
                    if continent_id is not None:
                        print "Cont_id %s, %s" % (continent_id, hash)
                        self._exec(conn, cur, self._remove_from_geodb_per_parent.format(continent_id=continent_id, hash=hash,
                                                                                        continents_table=continents_table))
                        self._exec(conn, cur, self._clear_simple_geom_at_level.format(level=0))

                    self._exec(conn, cur, self._setup_simplify_polygons.format(hash=hash,
                                                                               geodb_table='geodb_{hash}'.format(hash=hash),
                                                                               level=level))

                    attempt = 0
                    eps = epsilon[level][0]
                    mult = epsilon[level][1]
                    while total_points > max_points[level] and attempt < 10:
                        print "Iteration %d with eps=%.4f for %s level=%d points=%d" % \
                              (attempt, eps, ancestor_condition, level, total_points)
                        self._exec(conn, cur, self._simplify_polygons.format(
                            epsilon=eps, level=level,
                            hash=hash,
                            geodb_table='geodb_{hash}'.format(hash=hash)))
                        total_points = self._points_by_id(conn, cur, 'geodb_{hash}'.format(hash=hash), ancestor_condition)
                        eps *= mult
                        attempt += 1

                    if total_points <= max_points[level]:
                        print "Satisfied condition for eps=%.4f for %s on attempt=%d level=%d points=%d" % \
                              (eps, ancestor_condition, attempt, level, total_points)
                    else:
                        print "Not satisfied condition for eps=%.4f for %s on attempt=%d points=%d level=%d" % \
                              (eps, ancestor_condition, attempt, total_points, level)
                    # Copy or merge simple geoms in case of border cases between continents
                    if continent_id is None:
                        self._exec(conn, cur, self._copy_simplified_geoms.format(hash=hash))
                    else:
                        self._exec(conn, cur, self._union_simplified_geoms.format(hash=hash))

                    self._exec(conn, cur, self._drop_topologies.format(hash=hash))
                    self._exec(conn, cur, self._drop_geodb_per_parent.format(hash=hash))

                except Exception as e:
                    print "An error occured in parallel thread", e


        class Functor(_ParallelFunctor):
            def func(self, *args):
                simplify_level_parent(*args)

        def subtasks():
            for level in self.get_levels(with_cursor=False):
                if level <= max_level:
                    print "Enter simplification level %d" % level
                    for ancestor_id in self.stream_ordered_parents(level, with_cursor=False):
                        print "Simplifying children of " + str(ancestor_id)
                        if level == 0:
                            for cont in self.get_continents_id(continents_table, with_cursor=False):
                                yield (level, ancestor_id, cont)
                        else:
                            yield (level, ancestor_id)

        self._exec_parallel(subtasks(), Functor, threads)

        for level in self.get_levels(with_cursor=False):
            if level <= max_level:
                with self._conn.cursor() as cur:
                    self._exec(self._conn, cur, self._verify_simplification.format(level=level))

    def _points_by_id(self, conn, cur, geodb_table, ancestor_condition):
        try:
            cur.execute(self._sum_points_by_parent_id.format(geodb_table=geodb_table, ancestor_condition=ancestor_condition))
        finally:
            while conn.notices:
                print conn.notices.pop(0)
        return cur.fetchone()[0] or 0

    def destroy(self):
        with self._conn.cursor() as cur:
            cur.execute(self._drop_table_if_exists_sql.format(name="geodb"))
            self._conn.commit()

    def cancel_backend(self):
        with self._conn.cursor() as cur:
            cur.execute(self._cancel_backend)
            self._conn.commit()

    # if name is None data is fetched into memory
    def _hashed_cursor(self, name):
        if name:
            return self._conn.cursor(name + "_" + str(randint(0, long(1e18))))
        else:
            return self._conn.cursor()

    def get_by_self_id(self, self_id):
        with self._conn.cursor('get_by_self_id', psycopg2.extras.DictCursor) as cur:
            cur.execute(self._get_sql, (self_id,))
            return dict(cur.fetchone())

    def stream_for_level(self, level):
        with self._conn.cursor('stream_for_level', psycopg2.extras.DictCursor) as cur:
            cur.execute(self._get_by_level_sql, (level,))
            for record in cur:
                yield dict(record)

    def stream_topo_order(self, max_level):
        with self._conn.cursor('stream_topo_order', psycopg2.extras.DictCursor) as cur:
            if max_level is None:
                cur.execute(self._get_topo_order_sql)
            else:
                cur.execute(self._get_topo_order_max_level_sql.format(max_level=max_level))
            for record in cur:
                yield dict(record)

    def stream_all(self):
        with self._conn.cursor('stream_all', psycopg2.extras.DictCursor) as cur:
            cur.execute(self._get_all_sql)
            for record in cur:
                yield dict(record)

    def stream_disputed(self):
        with self._conn.cursor('stream_disputed', psycopg2.extras.DictCursor) as cur:
            cur.execute(self._get_disputed)
            for record in cur:
                yield dict(record)

    # parallel with hashed cursor
    def has_levels(self):
        with self._hashed_cursor('has_levels') as cur:
            cur.execute(self._has_levels_sql)
            return cur.fetchone()[0]

    def get_levels(self, with_cursor=True):
        with self._hashed_cursor('get_levels' if with_cursor else None) as cur:
            cur.execute(self._get_levels_sql)
            for record in cur:
                yield record[0]

    def get_continents_id(self, continents_table, with_cursor=True):
        with self._hashed_cursor('get_continents' if with_cursor else None) as cur:
            cur.execute(self._get_continents_sql.format(continents_table=continents_table))
            for record in cur:
                yield record[0]

    def stream_ordered_parents(self, level, with_cursor=True):
        with self._hashed_cursor('stream_ordered_parents' if with_cursor else None) as cur:
            cur.execute(self._get_ordered_parents.format(level=level))
            for record in cur:
                yield record[0]

    def stream_ordered_ids(self, level, with_cursor=True):
        with self._hashed_cursor('stream_ordered_ids' if with_cursor else None) as cur:
            cur.execute(self._get_self_id_for_level_sql.format(max_level=level))
            for record in cur:
                yield record[0]

    def fix_custom(self):
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self._custom_fix_countries_sql)

    def import_border(self, border_sql_filename, border_table_name, land_polygons_name, min_area):
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self._drop_table_if_exists_sql.format(name=land_polygons_name))
            self._exec(self._conn, cur, self._drop_table_if_exists_sql.format(name=border_table_name))
            print "Importing %s from %s..." % (land_polygons_name, border_sql_filename)
            self._exec_plsql_import(border_sql_filename)
            print "Done importing %s" % land_polygons_name
            print "Creating %s table..." % border_table_name
            self._exec(self._conn, cur, self._create_table_border_sql.format(border_name=border_table_name,
                                                                             land_polygons_name=land_polygons_name,
                                                                             min_area=min_area))
            print "Done creating %s" % border_table_name
            self._exec(self._conn, cur, self._drop_table_if_exists_sql.format(name=land_polygons_name))

    def import_continents(self, continents_sql_filename, continents_table_name):
        with self._conn.cursor() as cur:
            self._exec(self._conn, cur, self._drop_table_if_exists_sql.format(name=continents_table_name))
            print "Importing %s from %s..." % (continents_table_name, continents_sql_filename)
            self._exec_plsql_import(continents_sql_filename)
            print "Done importing %s" % continents_table_name

    def _exec_plsql_import(self, sql_file_path):
        tokens = [t.split('=') for t in self._psql_str.split()]
        conn_args = {token[0]: token[1] for token in tokens}
        d = dict(os.environ)
        d['PGPASSWORD'] = conn_args["password"]
        FNULL = open(os.devnull, 'w')
        work = subprocess.Popen(['psql',
                              "-f%s" % sql_file_path,
                              "-d%s" % conn_args["dbname"],
                              "-h%s" % conn_args["host"],
                              "-U%s" % conn_args["user"]
                              ], env=d, stdout=FNULL, stderr=subprocess.STDOUT)
        work.wait()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-p', default='host=localhost dbname=geodb user=postgres password=postgres', dest='psql')
    args = parser.parse_args()

    geodb = Geodb(args.psql)
    geoms = ['POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))', 'POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))']
    for i, geo in enumerate(geoms):
        geodb.insert(i, geo)
    geodb.build()
    for i in xrange(len(geoms)):
        print geodb.get_by_self_id(i)
    for record in geodb.stream_all():
        print record
    geodb.destroy()
