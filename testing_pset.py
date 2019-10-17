#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pset_1` package."""

import os
from tempfile import TemporaryDirectory
from unittest import TestCase
import pandas as pd
import tempfile

from pset_1.hash_str import hash_str, str_to_byte, get_csci_salt
from pset_1.io import atomic_write
from pset_1.__main__ import call_getuserid, parquet_conv


class FakeFileFailure(IOError):
    pass


class Main_Tests(TestCase):
    def test_parquet_conv(self):
        # with tempfile.TemporaryDirectory() as tempdirname:
        df = pd.DataFrame({"hashed_id": [1, 2, 3, 4, 5]})
        tf = tempfile.NamedTemporaryFile(delete=False, dir=os.getcwd(), suffix=".csv")
        tf.close()
        with open(tf.name) as temp:
            df.to_csv(temp.name + ".csv")
            result = parquet_conv(
                filename=temp.name, cwd=os.getcwd(), datasourceformat=".csv"
            )
            print(result)
            pd.testing.assert_frame_equal(df, result)


class HashTests(TestCase):
    def setUp(self):
        self.count = 0

    def test_decorator(self):
        @str_to_byte
        def a(x, y):
            if isinstance(x, bytes):
                self.count += 1
            if isinstance(y, bytes):
                self.count += 1
            if self.count == 2:
                return "expected result"

        self.assertEqual(a("test", "test"), "expected result")

    def test_basic(self):
        self.assertEqual(hash_str("world!", salt="hello, ").hex()[:6], "68e656")

    def test_getcsci(self):
        os.environ["test_envvar"] = "yes"
        environ_var = get_csci_salt(keyword="test_envvar", convert_to_bytes="no")
        self.assertEqual(environ_var, "yes")


class AtomicWriteTests(TestCase):
    def test_atomic_write(self):
        """Ensure file exists after being written successfully"""

        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "asdf.txt")

            with atomic_write(fp, "w") as f:
                assert not os.path.exists(fp)
                tmpfile = f.name
                f.write("asdf")

            assert not os.path.exists(tmpfile)
            assert os.path.exists(fp)

            with open(fp) as f:
                self.assertEqual(f.read(), "asdf")

    def test_atomic_failure(self):
        """Ensure that file does not exist after failure during write"""

        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "asdf.txt")

            with self.assertRaises(FakeFileFailure):
                # with self.assertRaises(FakeFileFailure):
                with atomic_write(fp, "w") as f:
                    tmpfile = f.name
                    assert os.path.exists(tmpfile)
                    raise FakeFileFailure

            assert not os.path.exists(tmpfile)
            assert not os.path.exists(fp)

    def test_file_exists(self):
        """Ensure an error is raised when a file exists"""

        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "asdf.txt")
            existing_file = open(os.path.join(tmp, "asdf.txt"), "w+")
            existing_file.close()

            with self.assertRaises(FileExistsError):
                with atomic_write(fp, "w") as f:
                    print("Running test...")
                    # assert not os.path.exists(fp)
                    # tmpfile = f.name
