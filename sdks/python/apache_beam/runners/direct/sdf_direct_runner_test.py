import traceback
import unittest

import logging
import os

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.io.iobase import RestrictionTracker


class OffsetBasedRangeTracker(RestrictionTracker):

  def __init__(self, start_position, stop_position):
    self.start_position = start_position
    self.stop_position = stop_position
    self.current_position = None

  def check_done(self):
    pass

  def current_restriction(self):
    return self.current_position

  def fraction_claimed(self):
    raise NotImplementedError

  def try_claim(self, position):
    if position >= self.start_position and position < self.stop_position:
      self.current_position = position
      return True

    return False

  def checkpoint(self):
    pass


class MySDF(beam.DoFn):

  def process(self, element, restriction_tracker=beam.DoFn.RestrictionTrackerParam, *args, **kwargs):
    logging.error('****** reading file %s with tracker %r', element, restriction_tracker)
    logging.error('****** reading range [%d, %d) from file %s',
                 restriction_tracker.start_position,
                 restriction_tracker.stop_position, element)
    file_name = element
    file = open(file_name, 'rb')
    file.seek(restriction_tracker.start_position)
    pos = restriction_tracker.start_position
    while restriction_tracker.try_claim(pos):
      line = file.readline()
      len_line = len(line)
      line = line.strip()
      if line is None:
        break
      yield line
      pos += len_line

    done = True

  def restriction_coder(self):
    return object()

  def initial_restriction(self, element):
    size = os.path.getsize(element)
    return OffsetBasedRangeTracker(0, size)

  def new_tracker(self):
    super(MySDF, self).new_tracker()

class LoggingDoFn(beam.DoFn):

  def process(self, element, *args, **kwargs):
    logging.error('Logging record: %s', element)
    yield element

class MyRegularDF(beam.DoFn):

  def process(self, element, *args, **kwargs):
    logging.error('Regular DF record: %s', element)
    yield element


class SDFDirectRunnerTest(unittest.TestCase):

  def test_direct_runner_bounded(self):
    try:
      p = Pipeline(runner='DirectRunner')
      (p
       | 'Create1' >> beam.Create([
          '/Users/chamikara/code/beam_py/sdf_direc_runner_remote/inputs/file1',
          '/Users/chamikara/code/beam_py/sdf_direc_runner_remote/inputs/file2',
          '/Users/chamikara/code/beam_py/sdf_direc_runner_remote/inputs/file3'])
       | 'SDF' >> beam.ParDo(MySDF())
       | 'LoggingDF1' >> beam.ParDo(LoggingDoFn()))

      (p
       | 'Create2' >> beam.Create([
          '/Users/chamikara/code/beam_py/sdf_direc_runner_remote/inputs/file1',
          '/Users/chamikara/code/beam_py/sdf_direc_runner_remote/inputs/file2',
          '/Users/chamikara/code/beam_py/sdf_direc_runner_remote/inputs/file3'])
       | 'RegularDF' >> beam.ParDo(MyRegularDF())
       | 'LoggingDF2' >> beam.ParDo(LoggingDoFn()))

      p.run()
    except:
      print(traceback.format_exc())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()