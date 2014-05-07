import functools
import random
import struct
import os

from nose import with_setup, SkipTest
from nose.tools import eq_ as eq, assert_raises
from rados import Rados
from rbd import (RBD, Image, ImageNotFound, InvalidArgument, ImageExists,
                 ImageBusy, ReadOnlyImage,
                 FunctionNotSupported, ArgumentOutOfRange,
                 RBD_FEATURE_LAYERING, RBD_FEATURE_STRIPINGV2)


rados = None
ioctx = None
features = None
IMG_NAME = 'foo'
IMG_SIZE = 8 << 20 # 8 MiB
IMG_ORDER = 22 # 4 MiB objects

def setup_module():
    global rados
    rados = Rados(conffile='')
    rados.connect()
    assert rados.pool_exists('rbd')
    global ioctx
    ioctx = rados.open_ioctx('rbd')
    global features
    features = os.getenv("RBD_FEATURES")
    if features is not None:
        features = int(features)

def teardown_module():
    global ioctx
    ioctx.__del__()
    global rados
    rados.shutdown()

def create_image():
    if features is not None:
        RBD().create(ioctx, IMG_NAME, IMG_SIZE, IMG_ORDER, old_format=False,
                     features=int(features))
    else:
        RBD().create(ioctx, IMG_NAME, IMG_SIZE, IMG_ORDER, old_format=True)

def remove_image():
    RBD().remove(ioctx, IMG_NAME)

def require_features(required_features):
    def wrapper(fn):
        def _require_features(*args, **kwargs):
            global features
            if features is None:
                raise SkipTest
            for feature in required_features:
                if feature & features != feature:
                    raise SkipTest
            return fn(*args, **kwargs)
        return functools.wraps(fn)(_require_features)
    return wrapper

def test_version():
    RBD().version()

def test_create():
    create_image()
    remove_image()

def check_default_params(format, order=None, features=None, stripe_count=None,
                         stripe_unit=None, exception=None):
    global rados
    global ioctx
    orig_vals = {}
    for k in ['rbd_default_format', 'rbd_default_order', 'rbd_default_features',
              'rbd_default_stripe_count', 'rbd_default_stripe_unit']:
        orig_vals[k] = rados.conf_get(k)
    try:
        rados.conf_set('rbd_default_format', str(format))
        if order is not None:
            rados.conf_set('rbd_default_order', str(order or 0))
        if features is not None:
            rados.conf_set('rbd_default_features', str(features or 0))
        if stripe_count is not None:
            rados.conf_set('rbd_default_stripe_count', str(stripe_count or 0))
        if stripe_unit is not None:
            rados.conf_set('rbd_default_stripe_unit', str(stripe_unit or 0))
        if exception is None:
            RBD().create(ioctx, IMG_NAME, IMG_SIZE)
            try:
                with Image(ioctx, IMG_NAME) as image:
                    eq(format == 1, image.old_format())

                    expected_order = order
                    if not order:
                        expected_order = 22
                    actual_order = image.stat()['order']
                    eq(expected_order, actual_order)

                    expected_features = features
                    if expected_features is None or format == 1:
                        expected_features = 0 if format == 1 else 3
                    eq(expected_features, image.features())

                    expected_stripe_count = stripe_count
                    if not expected_stripe_count or format == 1 or \
                           features & RBD_FEATURE_STRIPINGV2 == 0:
                        expected_stripe_count = 1
                    eq(expected_stripe_count, image.stripe_count())

                    expected_stripe_unit = stripe_unit
                    if not expected_stripe_unit or format == 1 or \
                           features & RBD_FEATURE_STRIPINGV2 == 0:
                        expected_stripe_unit = 1 << actual_order
                    eq(expected_stripe_unit, image.stripe_unit())
            finally:
                RBD().remove(ioctx, IMG_NAME)
        else:
            assert_raises(exception, RBD().create, ioctx, IMG_NAME, IMG_SIZE)
    finally:
        for k, v in orig_vals.iteritems():
            rados.conf_set(k, v)

def test_create_defaults():
    # basic format 1 and 2
    check_default_params(1)
    check_default_params(2)
    # default order still works
    check_default_params(1, 0)
    check_default_params(2, 0)
    # invalid order
    check_default_params(1, 11, exception=ArgumentOutOfRange)
    check_default_params(2, 11, exception=ArgumentOutOfRange)
    check_default_params(1, 65, exception=ArgumentOutOfRange)
    check_default_params(2, 65, exception=ArgumentOutOfRange)
    # striping and features are ignored for format 1
    check_default_params(1, 20, 0, 1, 1)
    check_default_params(1, 20, 3, 1, 1)
    check_default_params(1, 20, 0, 0, 0)
    # striping is ignored if stripingv2 is not set
    check_default_params(2, 20, 0, 1, 1 << 20)
    check_default_params(2, 20, RBD_FEATURE_LAYERING, 1, 1 << 20)
    check_default_params(2, 20, 0, 0, 0)
    # striping with stripingv2 is fine
    check_default_params(2, 20, RBD_FEATURE_STRIPINGV2, 1, 1 << 16)
    check_default_params(2, 20, RBD_FEATURE_STRIPINGV2, 10, 1 << 20)
    check_default_params(2, 20, RBD_FEATURE_STRIPINGV2, 10, 1 << 16)
    # make sure invalid combinations of stripe unit and order are still invalid
    check_default_params(2, 20, RBD_FEATURE_STRIPINGV2, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 10, 1 << 50, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 10, 100, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 0, 1, exception=InvalidArgument)
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 1, 0, exception=InvalidArgument)
    # 0 stripe unit and count are still ignored
    check_default_params(2, 22, RBD_FEATURE_STRIPINGV2, 0, 0)

def test_context_manager():
    with Rados(conffile='') as cluster:
        with cluster.open_ioctx('rbd') as ioctx:
            RBD().create(ioctx, IMG_NAME, IMG_SIZE)
            with Image(ioctx, IMG_NAME) as image:
                data = rand_data(256)
                image.write(data, 0)
                read = image.read(0, 256)
            RBD().remove(ioctx, IMG_NAME)
            eq(data, read)

def test_open_read_only():
    with Rados(conffile='') as cluster:
        with cluster.open_ioctx('rbd') as ioctx:
            RBD().create(ioctx, IMG_NAME, IMG_SIZE)
            data = rand_data(256)
            with Image(ioctx, IMG_NAME) as image:
                image.write(data, 0)
            with Image(ioctx, IMG_NAME, read_only=True) as image:
                read = image.read(0, 256)
                eq(data, read)
                assert_raises(ReadOnlyImage, image.write, data, 0)
            RBD().remove(ioctx, IMG_NAME)
            eq(data, read)

def test_remove_dne():
    assert_raises(ImageNotFound, remove_image)

def test_list_empty():
    eq([], RBD().list(ioctx))

@with_setup(create_image, remove_image)
def test_list():
    eq([IMG_NAME], RBD().list(ioctx))

@with_setup(create_image, remove_image)
def test_rename():
    rbd = RBD()
    rbd.rename(ioctx, IMG_NAME, IMG_NAME + '2')
    eq([IMG_NAME + '2'], rbd.list(ioctx))
    rbd.rename(ioctx, IMG_NAME + '2', IMG_NAME)
    eq([IMG_NAME], rbd.list(ioctx))

def rand_data(size):
    l = [random.Random().getrandbits(64) for _ in xrange(size/8)]
    return struct.pack((size/8)*'Q', *l)

def check_stat(info, size, order):
    assert 'block_name_prefix' in info
    eq(info['size'], size)
    eq(info['order'], order)
    eq(info['num_objs'], size / (1 << order))
    eq(info['obj_size'], 1 << order)

class TestImage(object):

    def setUp(self):
        self.rbd = RBD()
        create_image()
        self.image = Image(ioctx, IMG_NAME)

    def tearDown(self):
        self.image.close()
        remove_image()

    def test_stat(self):
        info = self.image.stat()
        check_stat(info, IMG_SIZE, IMG_ORDER)

    def test_write(self):
        data = rand_data(256)
        self.image.write(data, 0)

    def test_read(self):
        data = self.image.read(0, 20)
        eq(data, '\0' * 20)

    def test_large_write(self):
        data = rand_data(IMG_SIZE)
        self.image.write(data, 0)

    def test_large_read(self):
        data = self.image.read(0, IMG_SIZE)
        eq(data, '\0' * IMG_SIZE)

    def test_write_read(self):
        data = rand_data(256)
        offset = 50
        self.image.write(data, offset)
        read = self.image.read(offset, 256)
        eq(data, read)

    def test_read_bad_offset(self):
        assert_raises(InvalidArgument, self.image.read, IMG_SIZE + 1, IMG_SIZE)

    def test_resize(self):
        new_size = IMG_SIZE * 2
        self.image.resize(new_size)
        info = self.image.stat()
        check_stat(info, new_size, IMG_ORDER)

    def test_size(self):
        eq(IMG_SIZE, self.image.size())
        new_size = IMG_SIZE * 2
        self.image.resize(new_size)
        eq(new_size, self.image.size())

    def test_resize_down(self):
        new_size = IMG_SIZE / 2
        data = rand_data(256)
        self.image.write(data, IMG_SIZE / 2);
        self.image.resize(new_size)
        self.image.resize(IMG_SIZE)
        read = self.image.read(IMG_SIZE / 2, 256)
        eq('\0' * 256, read)

    def test_resize_bytes(self):
        new_size = IMG_SIZE / 2 - 5
        data = rand_data(256)
        self.image.write(data, IMG_SIZE / 2 - 10);
        self.image.resize(new_size)
        self.image.resize(IMG_SIZE)
        read = self.image.read(IMG_SIZE / 2 - 10, 5)
        eq(data[:5], read)
        read = self.image.read(IMG_SIZE / 2 - 5, 251)
        eq('\0' * 251, read)

    def test_copy(self):
        global ioctx
        data = rand_data(256)
        self.image.write(data, 256)
        self.image.copy(ioctx, IMG_NAME + '2')
        assert_raises(ImageExists, self.image.copy, ioctx, IMG_NAME + '2')
        copy = Image(ioctx, IMG_NAME + '2')
        copy_data = copy.read(256, 256)
        copy.close()
        self.rbd.remove(ioctx, IMG_NAME + '2')
        eq(data, copy_data)

    def test_lock_unlock(self):
        assert_raises(ImageNotFound, self.image.unlock, '')
        self.image.lock_exclusive('')
        assert_raises(ImageExists, self.image.lock_exclusive, '')
        assert_raises(ImageBusy, self.image.lock_exclusive, 'test')
        assert_raises(ImageExists, self.image.lock_shared, '', '')
        assert_raises(ImageBusy, self.image.lock_shared, 'foo', '')
        self.image.unlock('')

    def test_list_lockers(self):
        eq([], self.image.list_lockers())
        self.image.lock_exclusive('test')
        lockers = self.image.list_lockers()
        eq(1, len(lockers['lockers']))
        _, cookie, _ = lockers['lockers'][0]
        eq(cookie, 'test')
        eq('', lockers['tag'])
        assert lockers['exclusive']
        self.image.unlock('test')
        eq([], self.image.list_lockers())

        num_shared = 10
        for i in xrange(num_shared):
            self.image.lock_shared(str(i), 'tag')
        lockers = self.image.list_lockers()
        eq('tag', lockers['tag'])
        assert not lockers['exclusive']
        eq(num_shared, len(lockers['lockers']))
        cookies = sorted(map(lambda x: x[1], lockers['lockers']))
        for i in xrange(num_shared):
            eq(str(i), cookies[i])
            self.image.unlock(str(i))
        eq([], self.image.list_lockers())

    def test_diff_iterate(self):
        check_diff(self.image, 0, IMG_SIZE, None, [])
        self.image.write('a' * 256, 0)
        check_diff(self.image, 0, IMG_SIZE, None, [(0, 256, True)])
        self.image.write('b' * 256, 256)
        check_diff(self.image, 0, IMG_SIZE, None, [(0, 512, True)])
        self.image.discard(128, 256)
        check_diff(self.image, 0, IMG_SIZE, None, [(0, 512, True)])

        self.image.discard(0, 1 << IMG_ORDER)
