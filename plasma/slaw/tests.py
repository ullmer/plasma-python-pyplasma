import unittest, os
import plasma.slaw
from loam import *
from plasma.protein import Protein

TEST_DATA = './t/data/slaw'

class Slaw2ParseSimpleTestCase(unittest.TestCase):

    def testParseNil(self):
        fn = os.path.join(TEST_DATA, 'v2/obnil.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), obnil)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)

    def testParseTrue(self):
        fn = os.path.join(TEST_DATA, 'v2/obbool_true.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), obbool)
        self.assertTrue(x)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)

    def testParseFalse(self):
        fn = os.path.join(TEST_DATA, 'v2/obbool_false.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), obbool)
        self.assertFalse(x)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)

class Slaw2ParseRealTestCase(unittest.TestCase):

    def testParseUnt8(self):
        fn = os.path.join(TEST_DATA, 'v2/unt8.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), unt8)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, 250)

    def testParseInt8(self):
        fn = os.path.join(TEST_DATA, 'v2/int8.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), int8)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, -125)

    def testParseUnt16(self):
        fn = os.path.join(TEST_DATA, 'v2/unt16.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), unt16)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, 65000)

    def testParseInt16(self):
        fn = os.path.join(TEST_DATA, 'v2/int16.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), int16)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, -32000)

    def testParseUnt32(self):
        fn = os.path.join(TEST_DATA, 'v2/unt32.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), unt32)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, 4000000000)

    def testParseInt32(self):
        fn = os.path.join(TEST_DATA, 'v2/int32.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), int32)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, -2000000000)

    def testParseUnt64(self):
        fn = os.path.join(TEST_DATA, 'v2/unt64.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), unt64)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, 8000000000)

    def testParseInt64(self):
        fn = os.path.join(TEST_DATA, 'v2/int64.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), int64)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, -8000000000)

    def testParseFloat32(self):
        fn = os.path.join(TEST_DATA, 'v2/float32.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), float32)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, 1.2345678806304932)

    def testParseFloat64(self):
        fn = os.path.join(TEST_DATA, 'v2/float64.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), float64)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, 1.23456789)

class Slaw2ParseComplexTestCase(unittest.TestCase):

    def testParseUnt8c(self):
        fn = os.path.join(TEST_DATA, 'v2/unt8c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), unt8c)
        self.assertEqual(type(x.real), unt8)
        self.assertEqual(type(x.imag), unt8)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 250)
        self.assertEqual(x.imag, 1)

    def testParseInt8c(self):
        fn = os.path.join(TEST_DATA, 'v2/int8c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), int8c)
        self.assertEqual(type(x.real), int8)
        self.assertEqual(type(x.imag), int8)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 125)
        self.assertEqual(x.imag, -125)

    def testParseUnt16c(self):
        fn = os.path.join(TEST_DATA, 'v2/unt16c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), unt16c)
        self.assertEqual(type(x.real), unt16)
        self.assertEqual(type(x.imag), unt16)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 65000)
        self.assertEqual(x.imag, 1)

    def testParseInt16c(self):
        fn = os.path.join(TEST_DATA, 'v2/int16c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), int16c)
        self.assertEqual(type(x.real), int16)
        self.assertEqual(type(x.imag), int16)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 32000)
        self.assertEqual(x.imag, -32000)

    def testParseUnt32c(self):
        fn = os.path.join(TEST_DATA, 'v2/unt32c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), unt32c)
        self.assertEqual(type(x.real), unt32)
        self.assertEqual(type(x.imag), unt32)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 4000000000)
        self.assertEqual(x.imag, 1)

    def testParseInt32c(self):
        fn = os.path.join(TEST_DATA, 'v2/int32c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), int32c)
        self.assertEqual(type(x.real), int32)
        self.assertEqual(type(x.imag), int32)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 2000000000)
        self.assertEqual(x.imag, -2000000000)

    def testParseUnt64c(self):
        fn = os.path.join(TEST_DATA, 'v2/unt64c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), unt64c)
        self.assertEqual(type(x.real), unt64)
        self.assertEqual(type(x.imag), unt64)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 8000000000)
        self.assertEqual(x.imag, 1)

    def testParseInt64c(self):
        fn = os.path.join(TEST_DATA, 'v2/int64c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), int64c)
        self.assertEqual(type(x.real), int64)
        self.assertEqual(type(x.imag), int64)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 8000000000)
        self.assertEqual(x.imag, -8000000000)

    def testParseFloat32c(self):
        fn = os.path.join(TEST_DATA, 'v2/float32c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), float32c)
        self.assertEqual(type(x.real), float32)
        self.assertEqual(type(x.imag), float32)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 1.2345678806304932)
        self.assertEqual(x.imag, 1.2345679395506094e+18) # 1234567890987654321

    def testParseFloat64c(self):
        fn = os.path.join(TEST_DATA, 'v2/float64c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), float64c)
        self.assertEqual(type(x.real), float64)
        self.assertEqual(type(x.imag), float64)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.real, 1.23456789)
        self.assertEqual(x.imag, 1.2345678909876544e+18) # 1234567890987654321

class Slaw2ParseVectorTestCase(unittest.TestCase):

    def testParseV2Unt8(self):
        fn = os.path.join(TEST_DATA, 'v2/v2unt8.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), v2unt8)
        self.assertEqual(type(x.x), unt8)
        self.assertEqual(type(x.y), unt8)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.x, 250)
        self.assertEqual(x.y, 1)

    def testParseV3Unt8(self):
        fn = os.path.join(TEST_DATA, 'v2/v3unt8.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), v3unt8)
        self.assertEqual(type(x.x), unt8)
        self.assertEqual(type(x.y), unt8)
        self.assertEqual(type(x.z), unt8)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.x, 250)
        self.assertEqual(x.y, 1)
        self.assertEqual(x.z, 100)

    def testParseV4Unt8(self):
        fn = os.path.join(TEST_DATA, 'v2/v4unt8.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), v4unt8)
        self.assertEqual(type(x.x), unt8)
        self.assertEqual(type(x.y), unt8)
        self.assertEqual(type(x.z), unt8)
        self.assertEqual(type(x.w), unt8)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.x, 250)
        self.assertEqual(x.y, 1)
        self.assertEqual(x.z, 100)
        self.assertEqual(x.w, 10)

    def testParseV2Int16(self):
        fn = os.path.join(TEST_DATA, 'v2/v2int16.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), v2int16)
        self.assertEqual(type(x.x), int16)
        self.assertEqual(type(x.y), int16)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.x, 125)
        self.assertEqual(x.y, -125)

    def testParseV3Int32c(self):
        fn = os.path.join(TEST_DATA, 'v2/v3int32c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), v3int32c)
        self.assertEqual(type(x.x), int32c)
        self.assertEqual(type(x.y), int32c)
        self.assertEqual(type(x.z), int32c)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(x.x.real, 125)
        self.assertEqual(x.x.imag, -125)
        self.assertEqual(x.y.real, 1)
        self.assertEqual(x.y.imag, -1)
        self.assertEqual(x.z.real, 100)
        self.assertEqual(x.z.imag, -100)
        self.assertEqual(test_data, orig_data)

    def testParseV4Float64c(self):
        fn = os.path.join(TEST_DATA, 'v2/v4float64c.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), v4float64c)
        self.assertEqual(type(x.x), float64c)
        self.assertEqual(type(x.y), float64c)
        self.assertEqual(type(x.z), float64c)
        self.assertEqual(type(x.w), float64c)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x.x.real, 1.25)
        self.assertEqual(x.y.imag, 2.5)
        self.assertEqual(x.z.real, -3.7)
        self.assertEqual(x.w.imag, -10.0)

class Slaw2ParseMultiVectorTestCase(unittest.TestCase):

    def testParseM2Unt8(self):
        pass

    def testParseM3Unt8(self):
        pass

    def testParseM4Unt8(self):
        pass

    def testParseM5Unt8(self):
        pass

    def testParseM2Int16(self):
        pass

    def testParseM3Int32(self):
        pass

    def testParseM4Int64(self):
        pass

    def testParseM5Float64(self):
        pass

class Slaw2ParseNumericArrayTestCase(unittest.TestCase):

    def testParseUnt8Array(self):
        fn = os.path.join(TEST_DATA, 'v2/unt8_array.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(len(x), 3)
        self.assertEqual(type(x[0]), unt8)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x[0], 250)
        self.assertEqual(x[1], 150)
        self.assertEqual(x[2], 50)

    def testParseInt32cArray(self):
        fn = os.path.join(TEST_DATA, 'v2/int32c_array.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(len(x), 3)
        self.assertEqual(type(x[0]), int32c)
        self.assertEqual(type(x[0].real), int32)
        self.assertEqual(type(x[0].imag), int32)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x[0].real, 2000000000)
        self.assertEqual(x[0].imag, -2000000000)
        self.assertEqual(x[1].real, 400000000)
        self.assertEqual(x[1].imag, -400000000)
        self.assertEqual(x[2].real, 600000000)
        self.assertEqual(x[2].imag, -600000000)

    def testParseV2Int32cArray(self):
        fn = os.path.join(TEST_DATA, 'v2/v3int32c_array.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(len(x), 3)
        self.assertEqual(type(x[0]), v3int32c)
        self.assertEqual(type(x[0].x), int32c)
        self.assertEqual(type(x[0].y), int32c)
        self.assertEqual(type(x[0].z), int32c)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(x[1].x.real, 225)
        self.assertEqual(x[1].x.imag, -225)
        self.assertEqual(x[1].y.real, 2)
        self.assertEqual(x[1].y.imag, -2)
        self.assertEqual(x[1].z.real, 200)
        self.assertEqual(x[1].z.imag, -200)
        self.assertEqual(test_data, orig_data)

    def testParseM5Float64Array(self):
        pass

class Slaw2ParseStringTestCase(unittest.TestCase):

    def testParseWeeString(self):
        fn = os.path.join(TEST_DATA, 'v2/weestring.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), obstring)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, 'foo')

    def testParseFullString(self):
        fn = os.path.join(TEST_DATA, 'v2/fullstring.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), obstring)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(x, 'something awful')

class Slaw2ParseStructTestCase(unittest.TestCase):

    def testParseObCons(self):
        fn = os.path.join(TEST_DATA, 'v2/cons.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), obcons)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(type(x.left), obstring)
        self.assertEqual(x.left, 'left')
        self.assertEqual(type(x.right), int64)
        self.assertEqual(x.right, 100)

    def testParseObList(self):
        fn = os.path.join(TEST_DATA, 'v2/oblist.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), oblist)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(len(x), 3)
        self.assertEqual(type(x[0]), obstring)
        self.assertEqual(x[0], 'stuff and junk')
        self.assertEqual(type(x[1]), int64)
        self.assertEqual(x[1], 100)
        self.assertEqual(type(x[2]), numeric_array)
        self.assertEqual(len(x[2]), 3)
        self.assertEqual(x[2][0], 1)

    def testParseObMap(self):
        fn = os.path.join(TEST_DATA, 'v2/obmap.slaw')
        x = plasma.slaw.read_slaw_file(fn)
        self.assertEqual(type(x), obmap)
        test_data = x.to_slaw(2)
        orig_data = file(fn).read()[8:]
        self.assertEqual(test_data, orig_data)
        self.assertEqual(len(x), 3)
        y = x.keys()
        self.assertEqual(type(y), oblist)
        self.assertEqual(len(y), 3)
        self.assertEqual(y[0], 'first')
        self.assertEqual(y[1], 'second')
        self.assertEqual(y[2], 'third')
        self.assertEqual(type(x['first']), obstring)
        self.assertEqual(x['first'], 'some stuff')
        self.assertEqual(type(x['second']), v3int8)
        self.assertEqual(type(x['third']), oblist)
        self.assertEqual(len(x['third']), 2)

class Slaw2ParseProteinTestCase(unittest.TestCase):

    def testParseProtein(self):
        pass

    def testParseBackwardProtein(self):
        pass

if '__main__' == __name__:
    unittest.main()

