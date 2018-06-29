#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Tests for kafkaio module."""

import logging
import unittest

from apache_beam import Create
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.io.kafkaio import ReadFromKafka
from apache_beam.testing.test_pipeline import TestPipeline


class ReadFromKafkaTest(unittest.TestCase):

  def test_read(self):
    expected_data = ['aaa', 'bbb', 'ccc', 'ddd']

    with TestPipeline() as p:
      pc1 = (p
             | 'Create1' >> Create(['test'])
             | 'SDF' >> ReadFromKafka())

      assert_that(pc1, equal_to(expected_data))



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
