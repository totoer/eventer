
import os
import unittest
import shutil
import asyncio
from eventer.eventer import Eventer

LAG_TEST_DB_N1 = 'lag_test_db_n1'
LAG_TEST_DB_N2 = 'lag_test_db_n2'


class TestLag(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        os.makedirs(LAG_TEST_DB_N1)
        os.makedirs(LAG_TEST_DB_N2)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(LAG_TEST_DB_N1)
        shutil.rmtree(LAG_TEST_DB_N2)
        return super().tearDown()

    async def test_lag(self):
        loop = asyncio.get_running_loop()

        n1 = Eventer(log_workdir=LAG_TEST_DB_N1,
                     host='localhost', port=9190,
                     nodes=[('localhost', 9191,),], loop=loop)
        n2 = Eventer(log_workdir=LAG_TEST_DB_N2,
                     host='localhost', port=9191,
                     nodes=[('localhost', 9190,),], loop=loop)

        await n1.serve()

        d = 'Hello World'
        await n1.emit('test_event', d=d)

        await n2.serve()
        await asyncio.sleep(3)

        self.assertEqual(n2._event_log.pick.name, 'test_event')
