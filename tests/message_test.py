
import os
import unittest
from eventer.messages import Message, MType, decode_message


class TestEventLog(unittest.TestCase):

    def test_encode_decode(self):
        m = Message('node_1', MType.EVENT, (1, 2, 3, 4,))
        data = m.encode()
        m2 = decode_message(data=data)

        self.assertEqual(m.node_id, m2.node_id)
        self.assertEqual(m.m_type, m2.m_type)
        self.assertEqual(m.data, m2.data)
