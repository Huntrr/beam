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

"""A source and a sink for reading from and writing to a Kafka broker."""


from __future__ import absolute_import

from collections import namedtuple

from copy import copy

from kafka.client_async import KafkaClient

from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.transforms import PTransform, ParDo, DoFn
from apache_beam.transforms.core import RestrictionProvider

__all__ = ['ReadFromKafka', 'WriteToKafka']


TOPIC = 'topic'
PARITION = 'partition'
START_OFFSET = 'start_offset'


KafkaPartition = namedtuple(
    'KafkaPartition',
    'topic partition start_offset')

KafkaRecord = namedtuple(
    'KafkaRecord',
    'topic partition offset key value timestamp timestamp_type')


class KafkaRestrictionProvider(RestrictionProvider):

  # Restriction is the offset to start reading from.
  def initial_restriction(self, element):
    assert isinstance(element, KafkaPartition)
    return element.start_offset or 0

  # RestrictionTracker is a OffsetRestrictionTracker that reads from the
  # given offset to infinity.
  def create_tracker(self, restriction):
    start_offset = restriction
    return OffsetRestrictionTracker(start_offset, None)


class KafkaReadFn(DoFn):
  """A Splittable `DoFn` that reads from a Kafka broker.

  Input elements should be of type `KafkaPartition`.
  Output elements will be of type `KafkaRecord`.
  """

  def process(
      self, element, restriction_tracker=KafkaRestrictionProvider(),):
    assert isinstance(element, KafkaPartition)
    assert isinstance(restriction_tracker, OffsetRestrictionTracker)
    assert element.partition_id

    ## Assume record_iter to be the iterator of records from the broker starting
    ## at element.start_offset
    # for record in record_iter:
    #   if restriction_tracker.try_claim(<offset of record>):
    #     # generate and yield an object of type KafkaRecord


class KafkaExpandPartitionsFn(DoFn):
  """A `DoFn` that expands Kafka topics into partitions.

  Input elements should be of type `KafkaPartition`.
  Output elements will be of type `KafkaPartition`.
  """
  def process(self, element,):
    if isinstance(element, KafkaPartition):
      topic = element.topic
      partition = element.partition
      start_offset = element.start_offset
    else:
      topic = element
      partition = None
      start_offset = None

    start_offset = start_offset or 0
    if partition:
      yield KafkaPartition(
          topic=topic, partition=partition, start_offset=start_offset)

    else:
      # Getting partitions of the given topic.


    if element.partition_id:
      yield element
    else:
      assert not element.start_offset
      partitions = [] # TODO: get partitions from Kafka broker.
      for partition in partitions:
        element = copy(element)
        element.partition_id = partition
        yield element


class ReadFromKafka(PTransform):
  """A `PTransform` for reading from a Kafka broker.

  The input to this transform should be a `PCollection` of `KafkaPartition`
  objects.
  Output of this transform will be a `PCollection` of `KafkaRecord` objects.

  Note that a single Read transform may read data from multiple topics and from
  a selected subset of partitions from each topic.
  """
  def __init__(
      self,
      # A list of Kafka broker servers of the form '<IP address>:port'
      bootstrap_servers,
      # function that will be used to deserialize keys
      key_deserializer,
      # function that will be used to deserialize values
      value_deserializer,
      # a coder for keys
      key_coder,
      # a coder for values
      value_coder,
      # only messages with timestamps equal to or larger than this value will
      # be read.
      start_read_time,
  ):
    """Initialize the `ReadFromKafka` transform.
    """
    pass

  def expand(self, pcoll):
    return pcoll | ParDo(KafkaExpandPartitionsFn()) | ParDo(KafkaReadFn())


class KafkaWriteFn(DoFn):
  """A `DoFn` that writes elements to a given Kafka topic."""
  pass

class WriteToKafka(PTransform):
  """A `PTransform` for writing to a Kafka broker."""

  # See `ReadFromKafka` for parameter that are not described here.
  def __init__(
      self,
      bootstrap_servers,
      # function that will be used to serialize keys
      key_serializer,
      # function that will be used to serialize values
      value_serializer,
      # Kafka topic to write to.
      topic,
  ):
    r"""Initialize the `WriteToKafka` transform.
    """
    pass

  def expand(self, pcoll):
    return pcoll | ParDo(KafkaWriteFn())

class _KafkaUtil(object):

  def _KafkaUtil(self, brokers,):
    self._client = KafkaClient()

