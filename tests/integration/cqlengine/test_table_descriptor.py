# Copyright 2015 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tests the .table() descriptor functions as advertised
"""

import mock

from tests.integration.cqlengine.base import BaseCassEngTestCase
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.columns import Integer, Text


from tests.integration import PROTOCOL_VERSION

class TableCallTest(BaseCassEngTestCase):
    class BucketTableTest(Model):
        __table_name__ = "bucket_table_test"
        id = Integer(primary_key=True)
        value = Text()

    @classmethod
    def setUpClass(cls):
        sync_table(cls.BucketTableTest)

    @classmethod
    def tearDownClass(cls):
        drop_table(cls.BucketTableTest)

    def test_create_queryset(self):
        tmp = self.BucketTableTest.table("some_other_name")
        # queryset should have a this table registered
        assert tmp._table == "some_other_name"
        s = str(tmp)
        assert "some_other_name" in s


    def test_save_model_to_new_table(self):
        sync_table(self.BucketTableTest, name="bucket_2016")

        tmp = self.BucketTableTest.create(id=1, value="test")

        with mock.patch.object(self.session, 'execute') as m:
            tmp.table("bucket_2016").save()

        assert m.call_count > 0

        drop_table(self.BucketTableTest, name="bucket_2016")


