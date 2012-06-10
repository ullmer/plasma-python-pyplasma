import unittest, math
from loam import *
from loam.obnum import obnumber, obint
from plasma.exceptions import *

def main():
    unittest.main()

class LoamSimpleTestCase(unittest.TestCase):

    def testNil(self):
        x = obnil()
        self.assertIsNone(x.to_json(True))

    def testObTrue(self):
        x = obbool(True)
        self.assertTrue(x)
        self.assertTrue(x.to_json(True))

    def testObFalse(self):
        x = obbool(False)
        self.assertFalse(x)
        self.assertFalse(x.to_json(True))
        
class LoamNumTestCase(unittest.TestCase):

    def testUnt8(self):
        x = unt8(1)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(1, x.bytesize())
        self.assertEquals('B', x.get_format())
        self.assertRaises(ObInadequateClassException, unt8, -1)
        self.assertRaises(ObInadequateClassException, unt8, 256)

    def testInt8(self):
        x = int8(-1)
        self.assertFalse(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(1, x.bytesize())
        self.assertEquals('b', x.get_format())
        self.assertRaises(ObInadequateClassException, int8, -128)
        self.assertRaises(ObInadequateClassException, int8, 128)

    def testUnt16(self):
        x = unt16(1)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(16, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(2, x.bytesize())
        self.assertEquals('H', x.get_format())
        self.assertRaises(ObInadequateClassException, unt16, -1)
        self.assertRaises(ObInadequateClassException, unt16, 65536)

    def testInt16(self):
        x = int16(-1)
        self.assertFalse(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(16, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(2, x.bytesize())
        self.assertEquals('h', x.get_format())
        self.assertRaises(ObInadequateClassException, int16, -32768)
        self.assertRaises(ObInadequateClassException, int16, 32768)

    def testUnt32(self):
        x = unt32(1)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(32, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(4, x.bytesize())
        self.assertEquals('I', x.get_format())
        self.assertRaises(ObInadequateClassException, unt32, -1)
        self.assertRaises(ObInadequateClassException, unt32, 2**32)

    def testInt32(self):
        x = int32(-1)
        self.assertFalse(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(32, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(4, x.bytesize())
        self.assertEquals('i', x.get_format())
        self.assertRaises(ObInadequateClassException, int32, -1 * (2**31))
        self.assertRaises(ObInadequateClassException, int32, 2**31)

    def testUnt64(self):
        x = unt64(1)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(64, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(8, x.bytesize())
        self.assertEquals('Q', x.get_format())
        self.assertRaises(ObInadequateClassException, unt64, -1)

    def testInt64(self):
        x = int64(-1)
        self.assertFalse(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(64, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(8, x.bytesize())
        self.assertEquals('q', x.get_format())
        self.assertRaises(ObInadequateClassException, int64, -1 * (2**63))
        self.assertRaises(ObInadequateClassException, int64, 2**63)

    def testFloat32(self):
        x = float32(1.23456789)
        self.assertTrue(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(32, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(4, x.bytesize())
        self.assertEquals('f', x.get_format())

    def testFloat64(self):
        x = float64(1.23456789)
        self.assertTrue(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(64, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(8, x.bytesize())
        self.assertEquals('d', x.get_format())

    def testUnt8c(self):
        x = unt8c(0, 1)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(2, x.bytesize())
        self.assertEquals('2B', x.get_format())
        self.assertRaises(ObInadequateClassException, unt8c, 0, -1)
        self.assertRaises(ObInadequateClassException, unt8c, 0, 256)

    def testInt8c(self):
        x = int8c(0, -1)
        self.assertFalse(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(2, x.bytesize())
        self.assertEquals('2b', x.get_format())
        self.assertRaises(ObInadequateClassException, int8c, 0, -128)
        self.assertRaises(ObInadequateClassException, int8c, 0, 128)

    def testUnt16c(self):
        x = unt16c(0, 1)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(16, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(4, x.bytesize())
        self.assertEquals('2H', x.get_format())
        self.assertRaises(ObInadequateClassException, unt16c, 0, -1)
        self.assertRaises(ObInadequateClassException, unt16c, 0, 65536)

    def testInt16c(self):
        x = int16c(0, -1)
        self.assertFalse(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(16, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(4, x.bytesize())
        self.assertEquals('2h', x.get_format())
        self.assertRaises(ObInadequateClassException, int16c, 0, -32768)
        self.assertRaises(ObInadequateClassException, int16c, 0, 32768)

    def testUnt32c(self):
        x = unt32c(0, 1)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(32, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(8, x.bytesize())
        self.assertEquals('2I', x.get_format())
        self.assertRaises(ObInadequateClassException, unt32c, 0, -1)
        self.assertRaises(ObInadequateClassException, unt32c, 0, 2**32)

    def testInt32c(self):
        x = int32c(0, -1)
        self.assertFalse(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(32, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(8, x.bytesize())
        self.assertEquals('2i', x.get_format())
        self.assertRaises(ObInadequateClassException, int32c, 0, -1 * (2**31))
        self.assertRaises(ObInadequateClassException, int32c, 0, 2**31)

    def testUnt64c(self):
        x = unt64c(0, 1)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(64, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(16, x.bytesize())
        self.assertEquals('2Q', x.get_format())
        self.assertRaises(ObInadequateClassException, unt64c, 0, -1)

    def testInt64c(self):
        x = int64c(0, -1)
        self.assertFalse(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(64, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(16, x.bytesize())
        self.assertEquals('2q', x.get_format())
        self.assertRaises(ObInadequateClassException, int64c, 0, -1 * (2**63))
        self.assertRaises(ObInadequateClassException, int64c, 0, 2**63)

    def testFloat32c(self):
        x = float32c(1.23456789)
        self.assertTrue(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(32, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(8, x.bytesize())
        self.assertEquals('2f', x.get_format())

    def testFloat64c(self):
        x = float64c(1.23456789)
        self.assertTrue(x.is_float)
        self.assertTrue(x.is_signed)
        self.assertTrue(x.is_complex)
        self.assertEquals(64, x.bits)
        self.assertEquals(1, x.size)
        self.assertEquals(0, x.vtype)
        self.assertEquals(16, x.bytesize())
        self.assertEquals('2d', x.get_format())

class LoamIntTestCase(unittest.TestCase):

    pass

class LoamFloatTestCase(unittest.TestCase):

    pass

class LoamComplexTestCase(unittest.TestCase):

    pass

class LoamVectorTestCase(unittest.TestCase):

    def testV2unt8(self):
        x = v2unt8(3, 4)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(2, x.size)
        self.assertEquals(1, x.vtype)
        self.assertEquals(2, x.bytesize())
        self.assertEquals('2B', x.get_format())
        self.assertEquals(unt8, type(x.x))
        self.assertEquals(3, x.x)
        self.assertEquals(3, x[0])
        self.assertEquals(unt8, type(x.y))
        self.assertEquals(4, x.y)
        self.assertEquals(4, x[1])
        self.assertEquals(5, abs(x))
        y = x + v2int8(2, 8)
        self.assertEquals(v2int8, type(y))
        self.assertEquals(5, y.x)
        self.assertEquals(12, y.y)
        y = v2unt8(5, 12) - v2unt8(2, 8)
        self.assertEquals(v2unt8, type(y))
        self.assertEquals(3, y.x)
        self.assertEquals(4, y.y)
        y = x * 4
        self.assertEquals(v2unt8, type(y))
        self.assertEquals(12, y.x)
        self.assertEquals(16, y.y)
        y = x.dot(v2unt8(5, 12))
        self.assertEquals(unt8, type(y))
        self.assertEquals(63, y)
        y = x.cross(v2unt8(5, 12))
        self.assertEquals(v3unt8, type(y))
        self.assertEquals(0, y.x)
        self.assertEquals(0, y.y)
        self.assertEquals(16, y.z)
        y = v2unt8(1, 0).angle(v2unt8(0, 1))
        self.assertEquals(float64, type(y))
        self.assertAlmostEquals(math.pi / 2, y, 6)
        y = v2unt8(1, 0).normal(v2unt8(0, 1))
        self.assertEquals(v3float32, type(y))
        self.assertEquals(0, y.x)
        self.assertEquals(0, y.y)
        self.assertEquals(1, y.z)

    def testV3unt8(self):
        x = v3unt8(3, 4, 5)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(3, x.size)
        self.assertEquals(2, x.vtype)
        self.assertEquals(3, x.bytesize())
        self.assertEquals('3B', x.get_format())
        self.assertEquals(3, x.x)
        self.assertEquals(4, x.y)
        self.assertEquals(5, x.z)

    def testV4unt8(self):
        x = v4unt8(3, 4, 5, 6)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(4, x.size)
        self.assertEquals(3, x.vtype)
        self.assertEquals(4, x.bytesize())
        self.assertEquals('4B', x.get_format())
        self.assertEquals(3, x.x)
        self.assertEquals(4, x.y)
        self.assertEquals(5, x.z)
        self.assertEquals(6, x.w)

class LoamMultiVectorTestCase(unittest.TestCase):

    def testMV2unt8(self):
        x = mv2unt8(3, 4, 5, 6)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(2, x.size)
        self.assertEquals(4, x.vtype)
        self.assertEquals(4, x.bytesize())
        self.assertEquals('4B', x.get_format())
        self.assertEquals(3, x[0])
        self.assertEquals(3, x.coeff[0])
        self.assertEquals(3, x.u0)
        self.assertEquals(3, x.a)
        self.assertEquals(3, x.e)
        self.assertEquals(4, x.x)
        self.assertEquals(4, x.e1)
        self.assertEquals(4, x.u1[0])
        self.assertEquals(5, x.y)
        self.assertEquals(5, x.e2)
        self.assertEquals(5, x.u1[1])
        self.assertEquals(6, x.i)
        self.assertEquals(6, x.e12)
        x.u1[0] = 10
        self.assertEquals(10, x[1])

    def testMV3unt8(self):
        x = mv3unt8(3, 4, 5, 6, 7, 8, 9, 10)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(3, x.size)
        self.assertEquals(5, x.vtype)
        self.assertEquals(8, x.bytesize())
        self.assertEquals('8B', x.get_format())
        self.assertEquals(3, x.u0)
        self.assertEquals(3, x.a)
        self.assertEquals(3, x.e)
        self.assertEquals(4, x.x)
        self.assertEquals(4, x.e1)
        self.assertEquals(5, x.y)
        self.assertEquals(5, x.e2)
        self.assertEquals(6, x.z)
        self.assertEquals(6, x.e3)
        self.assertEquals(7, x.xy)
        self.assertEquals(7, x.e12)
        self.assertEquals(8, x.yz)
        self.assertEquals(8, x.e23)
        self.assertEquals(9, x.zx)
        self.assertEquals(9, x.e31)
        self.assertEquals(10, x.u3)
        self.assertEquals(10, x.i)
        self.assertEquals(10, x.e123)
        self.assertEquals(4, x.u1[0])
        self.assertEquals(3, len(x.u1))
        self.assertEquals(7, x.u2[0])

    def testMV4unt8(self):
        x = mv4unt8(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(4, x.size)
        self.assertEquals(6, x.vtype)
        self.assertEquals(16, x.bytesize())
        self.assertEquals('16B', x.get_format())

    def testMV5unt8(self):
        v = list(range(32))
        x = mv5unt8(*v)
        self.assertFalse(x.is_float)
        self.assertFalse(x.is_signed)
        self.assertFalse(x.is_complex)
        self.assertEquals(8, x.bits)
        self.assertEquals(5, x.size)
        self.assertEquals(7, x.vtype)
        self.assertEquals(32, x.bytesize())
        self.assertEquals('32B', x.get_format())

class LoamStringTestCase(unittest.TestCase):

    def testString(self):
        x = obstring('foobar')
        self.assertEquals(u'foobar', x.to_json(True))

class LoamStructTestCase(unittest.TestCase):

    def testCons(self):
        x = obcons(['key', 'value'])
        self.assertEquals('key', x.left)
        self.assertEquals('value', x.right)
        self.assertEquals(obstring, type(x.left))
        self.assertEquals(obstring, type(x.right))
        self.assertEquals(['key', 'value'], x.to_json(True))

    def testList(self):
        x = oblist(['one', 'two', 3, { 'four': 5 }])
        self.assertEquals(4, len(x))
        self.assertEquals('one', x[0])
        self.assertEquals(obstring, type(x[0]))
        self.assertTrue(isinstance(x[2], obint))
        self.assertEquals(obmap, type(x[3]))
        x[2:3] = ['seven', 'eight']
        self.assertEquals(5, len(x))
        self.assertEquals('eight', x[3])
        self.assertEquals(obstring, type(x[3]))
        y = x + ['a', 'b', 'c']
        self.assertEquals(oblist, type(y))
        self.assertEquals(8, len(y))
        self.assertEquals(obstring, type(y[5]))
        y = x[2:3]
        self.assertEquals(oblist, type(y))
        self.assertEquals(1, len(y))
        self.assertEquals('seven', y[0])
        y = x * int8(3)
        self.assertEquals(oblist, type(y))
        self.assertEquals(15, len(y))
        self.assertEquals('one', y[5])
        x.append('foo')
        self.assertEquals(6, len(x))
        self.assertEquals(obstring, type(x[5]))
        x.extend(['a', 'b', 'c'])
        self.assertEquals(9, len(x))
        self.assertEquals(obstring, type(x[6]))
        x.insert(1, ['a', 2, 3])
        self.assertEquals(10, len(x))
        self.assertEquals(oblist, type(x[1]))

    def testSearch(self):
        x = oblist(['foo', 'bar', 1, 'baz'])
        ix = x.search_ex('abc')
        self.assertEquals(-1, ix)
        self.assertEquals(int64, type(ix))
        ix = x.search_ex('bar')
        self.assertEquals(1, ix)
        self.assertEquals(int64, type(ix))
        ix = x.search_ex(['bar', 'foo'])
        self.assertEquals(-1, ix)
        self.assertEquals(int64, type(ix))
        ix = x.search_ex(['bar', 'baz'])
        self.assertEquals(1, ix)
        self.assertEquals(int64, type(ix))
        ix = x.search_ex(['bar', 'baz'], SEARCH_CONTIG)
        self.assertEquals(-1, ix)
        self.assertEquals(int64, type(ix))
        ix = x.search_ex([1, 'baz'], SEARCH_CONTIG)
        self.assertEquals(2, ix)
        self.assertEquals(int64, type(ix))

    def testMap(self):
        x = obmap({ 'one': 2, 'three': ['a', 'b', 3] })
        self.assertEquals(2, len(x))
        self.assertIn('one', x.keys())
        self.assertIn(2, x.values())
        self.assertTrue(isinstance(x['one'], obint))
        self.assertTrue(oblist, type(x['three']))
        x['foo'] = ['bar', 'baz', 'qux']
        self.assertEquals(oblist, type(x['foo']))
        items = x.items()
        self.assertEquals(oblist, type(items))
        self.assertEquals(3, len(items))
        self.assertEquals(obcons, type(items[0]))

if '__main__' == __name__:
    main()
