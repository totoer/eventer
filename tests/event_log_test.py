
import os
import shutil
import unittest
from eventer.event_log import EventLog
from eventer.messages import Event

TEST_APPEND_WORKDIR = 'test_appen'
TEST_GET_AFTER_WORKDIR = 'test_get_after'
TEST_STORE_RESTORE_WORKDIR = 'test_store_restore'
TEST_MAX_SIZE_WORKDIR = 'test_max_size'


def _clear_log_db_files():
    if os.path.exists(TEST_APPEND_WORKDIR):
        shutil.rmtree(TEST_APPEND_WORKDIR)

    if os.path.exists(TEST_GET_AFTER_WORKDIR):
        shutil.rmtree(TEST_GET_AFTER_WORKDIR)

    if os.path.exists(TEST_STORE_RESTORE_WORKDIR):
        shutil.rmtree(TEST_STORE_RESTORE_WORKDIR)

    if os.path.exists(TEST_MAX_SIZE_WORKDIR):
        shutil.rmtree(TEST_MAX_SIZE_WORKDIR)


class TestEventLog(unittest.TestCase):

    def setUp(self) -> None:
        _clear_log_db_files()
        os.makedirs(TEST_APPEND_WORKDIR)
        os.makedirs(TEST_GET_AFTER_WORKDIR)
        os.makedirs(TEST_STORE_RESTORE_WORKDIR)
        os.makedirs(TEST_MAX_SIZE_WORKDIR)
        return super().setUp()

    def tearDown(self) -> None:
        _clear_log_db_files()
        return super().tearDown()

    def test_append(self):
        e = Event('test', {'foo': 'data'})
        el = EventLog(TEST_APPEND_WORKDIR, 10)
        ok = el.append('node_1', event=e)
        self.assertTrue(ok, 'Append result not ok')

    def test_store_restore(self):
        e = Event('test', {'foo': 'data'})
        el = EventLog(TEST_STORE_RESTORE_WORKDIR, 10)
        el.append('node_1', event=e)

        del el

        el = EventLog(TEST_STORE_RESTORE_WORKDIR, 10)
        self.assertEqual(e.name, el.pick.name, 'dosn\'t restore event')
        self.assertEqual(e.args, el.pick.args, 'dosn\'t restore event')

    def test_max_size(self):
        e1 = Event('test', {'foo': 'data'})
        e2 = Event('test', {'foo': 'data'})
        e3 = Event('test', {'foo': 'data'})
        el = EventLog(TEST_MAX_SIZE_WORKDIR, 2)
        el.append('node_1', event=e1)
        el.append('node_1', event=e2)
        el.append('node_1', event=e3)

        self.assertEqual(len(el._log), 2, 'Wrong size of log')
