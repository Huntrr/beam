import traceback
import unittest

import logging
import os
from threading import Lock

from apache_beam.io import filebasedsource_test

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.runners.direct import transform_evaluator
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.io.iobase import RestrictionTracker


class OffsetBasedRangeTracker(RestrictionTracker):

  def __init__(self, start_position, stop_position):
    self.start_position = start_position
    self.stop_position = stop_position
    self.current_position = None
    self._lock = Lock()

  def check_done(self):
    pass

  def current_restriction(self):
    return self.current_position

  def fraction_claimed(self):
    raise NotImplementedError

  def try_claim(self, position):
    with self._lock:
      if position >= self.start_position and position < self.stop_position:
        self.current_position = position
        return True

      return False

  def checkpoint(self):
    with self._lock:
      previous_stop_position = self.stop_position
      self.stop_position = self.start_position if self.current_position == None else self.current_position + 1
      return (self.stop_position, previous_stop_position)


class MySDF(beam.DoFn):

  def process(self, element, restriction_tracker=beam.DoFn.RestrictionTrackerParam, *args, **kwargs):
    # logging.error('****** reading file %s with tracker %r', element, restriction_tracker)
    # logging.error('****** reading range [%d, %d) from file %s',
    #              restriction_tracker.start_position,
    #              restriction_tracker.stop_position, element)
    file_name = element
    file = open(file_name, 'rb')
    pos = restriction_tracker.start_position
    if restriction_tracker.start_position > 0:
      file.seek(restriction_tracker.start_position - 1)
      line = file.readline()
      pos = pos - 1 + len(line)


    while restriction_tracker.try_claim(pos):
      line = file.readline()
      len_line = len(line)
      line = line.strip()
      if not line:
        break

      if line is None:
        break
      yield line
      pos += len_line

  def restriction_coder(self):
    return object()

  def initial_restriction(self, element):
    size = os.path.getsize(element)
    return (0, size)

  def new_tracker(self, restriction):
    return OffsetBasedRangeTracker(restriction[0], restriction[1])

  def split(self, element, restriction):
    return [restriction,]

class LoggingDoFn(beam.DoFn):

  def process(self, element, *args, **kwargs):
    # logging.error('Logging record: %s', element)
    yield element

class MyRegularDF(beam.DoFn):

  def process(self, element, *args, **kwargs):
    logging.error('Regular DF record: %s', element)
    yield element


class SDFDirectRunnerTest(unittest.TestCase):

  def setUp(self):
    super(SDFDirectRunnerTest, self).setUp()
    self._default_max_num_outputs = transform_evaluator._ProcessElemenetsEvaluator.DEFAULT_MAX_NUM_OUTPUTS

  def run_sdf_read_pipeline(self, num_files, num_records_per_file):
    expected_data = []
    file_names = []
    for _ in range(num_files):
      new_file_name, new_expected_data = filebasedsource_test.write_data(
          num_records_per_file)
      assert len(new_expected_data) == num_records_per_file
      file_names.append(new_file_name)
      expected_data.extend(new_expected_data)

    # logging.info('file names: %r', file_names)
    # logging.info('expected data: %r', expected_data)
    p = Pipeline(runner='DirectRunner')
    pc1 = (p
           | 'Create1' >> beam.Create(file_names)
           | 'SDF' >> beam.ParDo(MySDF())
           | 'LoggingDF1' >> beam.ParDo(LoggingDoFn()))

    assert_that(pc1, equal_to(expected_data))
    p.run().wait_until_finish()


  def test_direct_runner_sdf_no_checkpoint_single_element(self):
    self.run_sdf_read_pipeline(
        1,
        int(self._default_max_num_outputs * 0.2))

  def test_direct_runner_sdf_one_checkpoint_single_element(self):
    self.run_sdf_read_pipeline(
        1,
        int(self._default_max_num_outputs * 1.2))

  def test_direct_runner_sdf_multiple_checkpoints_single_element(self):
    self.run_sdf_read_pipeline(
        1,
        int(self._default_max_num_outputs * 3.2))

  def test_direct_runner_sdf_no_checkpoint_multiple_element(self):
    self.run_sdf_read_pipeline(
        5,
        int(self._default_max_num_outputs * 0.2))

  def test_direct_runner_sdf_one_checkpoint_multiple_element(self):
    self.run_sdf_read_pipeline(
        5,
        int(self._default_max_num_outputs * 1.2))

  def test_direct_runner_sdf_multiple_checkpoints_multiple_element(self):
    self.run_sdf_read_pipeline(
        5,
        int(self._default_max_num_outputs * 3.2))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()