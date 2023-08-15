
import os
import unittest
import shutil
import asyncio
from eventer.eventer import Eventer

ONE_NODE_TEST_DB = 'test_db'
TWO_NODE_TEST_DB_N1 = 'two_node_test_db_n1'
TWO_NODE_TEST_DB_N2 = 'two_node_test_db_n2'
THREE_NODE_TEST_DB_N1 = 'three_node_test_db_n1'
THREE_NODE_TEST_DB_N2 = 'three_node_test_db_n2'
THREE_NODE_TEST_DB_N3 = 'three_node_test_db_n3'


class TestEventer(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        os.makedirs(ONE_NODE_TEST_DB)
        os.makedirs(TWO_NODE_TEST_DB_N1)
        os.makedirs(TWO_NODE_TEST_DB_N2)
        os.makedirs(THREE_NODE_TEST_DB_N1)
        os.makedirs(THREE_NODE_TEST_DB_N2)
        os.makedirs(THREE_NODE_TEST_DB_N3)
        return super().setUp()

    def tearDown(self) -> None:
        shutil.rmtree(ONE_NODE_TEST_DB)
        shutil.rmtree(TWO_NODE_TEST_DB_N1)
        shutil.rmtree(TWO_NODE_TEST_DB_N2)
        shutil.rmtree(THREE_NODE_TEST_DB_N1)
        shutil.rmtree(THREE_NODE_TEST_DB_N2)
        shutil.rmtree(THREE_NODE_TEST_DB_N3)
        return super().tearDown()

    async def test_one_node(self):
        loop = asyncio.get_running_loop()

        n = Eventer(log_workdir=ONE_NODE_TEST_DB,
                    host='localhost', port=9090, nodes=[], loop=loop)
        await n.serve()

        a = 'Hello'
        b = 'World'
        check_result = f'{a} {b}'
        result = loop.create_future()

        async def callback(a: str, b: str):
            result.set_result(f'{a} {b}')

        n.on('test_event', callback)

        await n.emit('test_event', a=a, b=b)

        await result
        self.assertEqual(check_result, result.result())

    async def test_two_node(self):
        loop = asyncio.get_running_loop()

        n1 = Eventer(log_workdir=TWO_NODE_TEST_DB_N1,
                     host='localhost', port=9190,
                     nodes=[('localhost', 9191,),], loop=loop)
        n2 = Eventer(log_workdir=TWO_NODE_TEST_DB_N2,
                     host='localhost', port=9191,
                     nodes=[('localhost', 9190,),], loop=loop)

        await n1.serve()
        await n2.serve()

        await asyncio.sleep(3)

        a = 'Hello'
        b = 'World'
        check_result = f'{a} {b}'
        result = loop.create_future()

        async def callback(a: str, b: str):
            result.set_result(f'{a} {b}')

        n1.on('test_event', callback)

        await n2.emit('test_event', a=a, b=b)

        await result
        self.assertEqual(check_result, result.result())

    async def test_three_node(self):
        loop = asyncio.get_running_loop()

        n1 = Eventer(log_workdir=THREE_NODE_TEST_DB_N1,
                     host='localhost', port=9290,
                     nodes=[('localhost', 9291,), ('localhost', 9292,),], loop=loop)
        n2 = Eventer(log_workdir=THREE_NODE_TEST_DB_N2,
                     host='localhost', port=9291,
                     nodes=[('localhost', 9290,), ('localhost', 9292,),], loop=loop)
        n3 = Eventer(log_workdir=THREE_NODE_TEST_DB_N3,
                     host='localhost', port=9292,
                     nodes=[('localhost', 9290,), ('localhost', 9291,),], loop=loop)

        await n1.serve()
        await n2.serve()
        await n3.serve()

        await asyncio.sleep(3)

        a = 'Hello'
        b = 'World'
        check_result = f'{a} {b}'
        result1 = loop.create_future()
        result2 = loop.create_future()
        result3 = loop.create_future()

        async def callback1(a: str, b: str):
            result1.set_result(f'{a} {b}')

        async def callback2(a: str, b: str):
            result2.set_result(f'{a} {b}')

        async def callback3(a: str, b: str):
            result3.set_result(f'{a} {b}')

        n1.on('test_event', callback1)
        n2.on('test_event', callback2)
        n3.on('test_event', callback3)

        await n3.emit('test_event', a=a, b=b)

        await result1
        await result2
        await result3
        self.assertEqual(check_result, result1.result())
        self.assertEqual(check_result, result2.result())
        self.assertEqual(check_result, result3.result())
