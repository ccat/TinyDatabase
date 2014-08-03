
import unittest

import set

class ExtensionalDefinitionTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_extensional_definition(self):
        setA = Set([1,2,3,4,5])
        self.assertTrue(setA.is_element(1))
        self.assertTrue(1 in setA)

    def test_union(self):
        setA = Set([1,2,3,4,5])
        setB = Set([4,5,6,7,8,9,10])

        setC = setA.union(setB)
        for i in range(1,10):
            self.assertTrue(i in setC)

        setC = setA + setB
        for i in range(1,10):
            self.assertTrue(i in setC)

    def test_intersection(self):
        setA = Set([1,2,3,4,5])
        setB = Set([4,5,6,7,8,9,10])

        setC = setA.intersection(setB)
        for i in range(1,3):
            self.assertTrue(not i in setC)
        for i in range(4,5):
            self.assertTrue(i in setC)
        for i in range(6,10):
            self.assertTrue(not i in setC)

    def test_complement(self):
        setA = Set([1,2,3,4,5])
        setB = Set([4,5,6,7,8,9,10])

        setC = setA.complement(setB)
        for i in range(1,3):
            self.assertTrue(i in setC)
        for i in range(4,10):
            self.assertTrue(not i in setC)

        setC = setA - setB
        for i in range(1,3):
            self.assertTrue(i in setC)
        for i in range(4,10):
            self.assertTrue(not i in setC)

    def test_relation(self):
        setA = Set([1,2,3,4,5])
        setB = Set([4,5,6,7,8,9,10])

        setC = setA.product(setB)
        for x in range(1,5):
            for y in range(4,10):
                self.assertTrue((x,y) in setC)

class IntensionalDefinitionTest(unittest.TestCase):

    def setUp(self):
        pass
