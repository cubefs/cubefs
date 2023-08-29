import os
import unittest


class MyTestCase(unittest.TestCase):
    def test_something(self):
        filename = "/mnt/cfs/text.txt"
        try:
            os.remove(filename)
        except Exception as e:
            pass
        with open(filename) as f:
            f.write("nihao")


if __name__ == '__main__':
    unittest.main()
