
import os
import unittest
import shutil
import asyncio
from eventer.eventer import Eventer

MISS_MASTER_TEST_DB_N1 = 'miss_master_test_db_n1'
MISS_MASTER_TEST_DB_N2 = 'miss_master_test_db_n2'


class TestMissMaster(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        os.makedirs(MISS_MASTER_TEST_DB_N1)
        os.makedirs(MISS_MASTER_TEST_DB_N2)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(MISS_MASTER_TEST_DB_N1)
        shutil.rmtree(MISS_MASTER_TEST_DB_N2)
        return super().tearDown()

    async def test_lag(self):
        loop = asyncio.get_running_loop()

        n1 = Eventer(log_workdir=MISS_MASTER_TEST_DB_N1,
                     host='localhost', port=9190,
                     nodes=[('localhost', 9191,),], loop=loop)
        n2 = Eventer(log_workdir=MISS_MASTER_TEST_DB_N2,
                     host='localhost', port=9191,
                     nodes=[('localhost', 9190,),], loop=loop)
