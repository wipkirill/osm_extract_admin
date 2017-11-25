import os
import shutil
import shapely.wkb as wkblib

from exceptions import RuntimeError

class Writer(object):
    def __call__(self, self_id, parent_id, path, data):
        raise NotImplementedError('__call__ not implemented')


class SimpleWriter(Writer):
    def __init__(self, path_prefix):
        super(SimpleWriter, self).__init__()
        self._path_prefix = path_prefix

        if os.path.exists(path_prefix):
            raise ValueError('Path %s already exists' % path_prefix)

        self._make_dir_path(path_prefix)

    def _make_dir_path(self, path):
        try:
            os.makedirs(path)
        except OSError:
            if not os.path.isdir(path):
                raise

    def __call__(self, path, data):
        self._make_dir_path(os.path.join(self._path_prefix, path))
        for key, value in data.iteritems():
            with open(os.path.join(self._path_prefix, path, "metadata." + key), "w+") as f:
                f.write(str(value))


def read_tags(path):
    tag_file = os.path.join(path, "metadata.tags")
    if not os.path.exists(tag_file):
        raise RuntimeError('No metadata.tags file at %s' % path)
    try:
        tag_str = open(tag_file, 'r').read()
        tags = eval(tag_str)
    except Exception as e:
        raise RuntimeError("Cannot deserialize set of tags %s at %s: e" % (tag_str, path, e))
    return {item[0]: item[1] for item in tags}


def read_level(path):
    lvl_file = os.path.join(path, "metadata.level")
    if not os.path.exists(lvl_file):
        raise RuntimeError('No metadata.level file at %s' % path)
    return int(open(lvl_file, 'r').read())


def read_geometry(path, simplified=False):
    ext = "simple_geom" if simplified else "geom"
    geom_file = os.path.join(path, "metadata." + ext)
    if not os.path.exists(geom_file):
        raise RuntimeError('No metadata geometry file at %s' % geom_file)
    try:
        wkb = open(geom_file, 'r').read()
        geom = wkblib.loads(wkb, hex=True)
    except Exception as e:
        raise RuntimeError("Failed to load geometry", e.message, geom_file)

    return geom


def read_disputed(path):
    disp_file = os.path.join(path, "metadata.disputed")
    if not os.path.exists(disp_file):
        raise RuntimeError('No metadata.tags file at %s' % path)
    try:
        disp_str = open(disp_file, 'r').read()
        disputed_list = eval(disp_str)
    except Exception as e:
        raise RuntimeError("Cannot deserialize set of tags %s at %s: e" % (disp_str, path, e))
    return disputed_list


class DummyWriter(Writer):
    def __init__(self):
        super(DummyWriter, self).__init__()

    def __call__(self, *args, **kwargs):
        print 'Calling with', args, kwargs


class Serializer(object):
    def __init__(self, writer):
        self._parent_map = {}
        self._roots = set()
        self._writer = writer

    def _get_parents_path(self, self_id, parent_id):
        if self_id is None:
            raise ValueError('self_id %s and parent_id %s is not valid' % (self_id, parent_id))

        if self_id == parent_id:
            raise ValueError('self_id %s and parent_id %s are equal' % (self_id, parent_id))

        if self_id in self._parent_map or self_id in self._roots:
            raise ValueError(
                'self_id %s with parent_id %s was seen before' % (
                    parent_id, self_id))

        if parent_id is not None and parent_id not in self._parent_map and parent_id not in self._roots:
            raise ValueError('parent_id %s with self_id %s was not seen before' % (parent_id, self_id))

        if parent_id is not None:
            self._parent_map[self_id] = parent_id
        else:
            self._roots.add(self_id)

        parents = [self_id]
        while self_id in self._parent_map:
            self_id = self._parent_map[self_id]
            parents.append(self_id)

        return os.path.join(*[str(p) for p in reversed(parents)])

    def write(self, self_id, parent_id, data):
        if data is None:
            raise ValueError('null data provided')
        path = self._get_parents_path(self_id, parent_id)
        self._writer(path, data)


if __name__ == '__main__':
    s = Serializer(DummyWriter())
    s.write(2, None, {'tags': {'c': 'd'}, 'wkt': 'POINT(2 2)'})
    s.write(1, 2, {'tags': {'a': 'b'}, 'wkt': 'POINT(1 1)'})
    s.write(0, 1, {'tags': {'foo': 'bar'}, 'wkt': 'POINT(0 0)'})
    s = Serializer(SimpleWriter('serializer_data_test'))
    s.write(2, None, {'tags': {'c': 'd'}, 'wkt': 'POINT(2 2)'})
    s.write(1, 2, {'tags': {'a': 'b'}, 'wkt': 'POINT(1 1)'})
    s.write(0, 1, {'tags': {'foo': 'bar'}, 'wkt': 'POINT(0 0)'})
    shutil.rmtree('serializer_data_test')
