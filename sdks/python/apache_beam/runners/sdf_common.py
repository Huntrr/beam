import uuid

from threading import Lock, Timer

from apache_beam.io.iobase import RestrictionTracker
from apache_beam.runners.direct.evaluation_context import DirectStepContext
from apache_beam.runners.direct.evaluation_context import DirectUnmergedState
from apache_beam.runners.direct.util import KeyedWorkItem
from apache_beam.runners.direct.watermark_manager import WatermarkManager
from apache_beam.transforms.trigger import _ValueStateTag

from apache_beam.coders import typecoders

import apache_beam as beam
from apache_beam import pvalue, TimeDomain

from apache_beam.transforms.ptransform import PTransform

from apache_beam.runners.common import DoFnInvoker, OutputProcessor, DoFnContext
from apache_beam.runners.common import DoFnSignature

from apache_beam.transforms.core import ParDo

from apache_beam.pipeline import AppliedPTransform, PTransformOverride
from apache_beam.utils.windowed_value import WindowedValue


class SplittableParDoOverride(PTransformOverride):

  def get_matcher(self):
    def _matcher(applied_ptransform):
      assert isinstance(applied_ptransform, AppliedPTransform)
      transform = applied_ptransform.transform
      if isinstance(transform, ParDo):
        signature = DoFnSignature(transform.fn)
        return signature.is_splittable_dofn()

    return _matcher

  def get_replacement_transform(self, ptransform):
    assert isinstance(ptransform, ParDo)
    do_fn = ptransform.fn
    signature = DoFnSignature(do_fn)
    if signature.is_splittable_dofn():
      return SplittableParDo(ptransform)
    else:
      return ptransform


class SplittableParDo(PTransform):

  def __init__(self, ptransform):
    assert isinstance(ptransform, ParDo)
    self._ptransform = ptransform

  def expand(self, input):
    sdf = self._ptransform.fn
    signature = DoFnSignature(sdf)
    invoker = DoFnInvoker.create_invoker(signature, process_invocation=False)

    element_coder = typecoders.registry.get_coder(input.element_type)
    restriction_coder = invoker.invoke_restriction_coder()

    keyed_elements = (input
                      | 'pair' >> ParDo(PairWithRestrictionFn(sdf))
                      | 'split' >> ParDo(SplitRestrictionFn(sdf))
                      | 'explode' >> ParDo(ExplodeWindowsFn())
                      | 'random' >> ParDo(RandomUniqueKeyFn()))

    return keyed_elements | ProcessKeyedElements(
        sdf, element_coder, restriction_coder, input.windowing,
        None, None, None)


class PairWithRestrictionFn(beam.DoFn):

  def __init__(self, do_fn):
    signature = DoFnSignature(do_fn)
    self._invoker = DoFnInvoker.create_invoker(
        signature, process_invocation=False)

  def process(self, element, *args, **kwargs):
    initial_restriction = self._invoker.invoke_initial_restriction(element)
    yield ElementAndRestriction(element, initial_restriction)


class SplitRestrictionFn(beam.DoFn):

  def __init__(self, do_fn):
    signature = DoFnSignature(do_fn)
    self._invoker = DoFnInvoker.create_invoker(
        signature, process_invocation=False)

  def process(self, element_and_restriction, *args, **kwargs):
    element = element_and_restriction.element
    restriction = element_and_restriction.restriction
    restriction_parts = self._invoker.invoke_split(
        element,
        restriction)
    for part in restriction_parts:
      yield ElementAndRestriction(element, part)


class ExplodeWindowsFn(beam.DoFn):

  def process(self, element, window=beam.DoFn.WindowParam, *args, **kwargs):
    yield element


class RandomUniqueKeyFn(beam.DoFn):

  def process(self, element, *args, **kwargs):
    yield (uuid.uuid4().bytes, element)


class ProcessKeyedElements(PTransform):

  def __init__(
      self, sdf, element_coder, restriction_coder, windowing_strategy,
      side_inputs, main_output_tag, additional_output_tags):
    self.sdf = sdf
    self.elemenet_coder = element_coder
    self.restriction_coder = restriction_coder
    self.windowing_strategy = windowing_strategy

    # self.side_inputs = side_inputs
    # self.main_output_tag = main_output_tag
    # self.additional_output_tags = additional_output_tags

  def expand(self, pcoll):
    return pvalue.PCollection(pcoll.pipeline)


class ProcessKeyedElementsViaKeyedWorkItemsOverride(PTransformOverride):

  def get_matcher(self):
    def _matcher(applied_ptransform):
      return isinstance(
          applied_ptransform.transform, ProcessKeyedElements)

    return _matcher

  def get_replacement_transform(self, ptransform):
    return ProcessKeyedElementsViaKeyedWorkItems(ptransform)


class ProcessKeyedElementsViaKeyedWorkItems(PTransform):

  def __init__(self, process_keyed_elements_transform):
    self._process_keyed_elements_transform = process_keyed_elements_transform

  def expand(self, pcoll):
    return pcoll | GBKIntoKeyedWorkItems() | ProcessElements(
        self._process_keyed_elements_transform)


class GBKIntoKeyedWorkItems(PTransform):

  def expand(self, pcoll):
    return pvalue.PCollection(pcoll.pipeline)


class ProcessElements(PTransform):

  def __init__(self, process_keyed_elements_transform):
    self._process_keyed_elements_transform = process_keyed_elements_transform
    self.sdf = self._process_keyed_elements_transform.sdf

  def expand(self, pcoll):
    return pvalue.PCollection(pcoll.pipeline)

  def new_process_fn(self, sdf):
    return ProcessFn(
        sdf,
        self._process_keyed_elements_transform.elemenet_coder,
        self._process_keyed_elements_transform.restriction_coder,
        self._process_keyed_elements_transform.windowing_strategy)


class ElementAndRestriction(object):

  def __init__(self, element, restriction):
    self.element = element
    self.restriction = restriction


class ProcessFn(beam.DoFn):

  def __init__(self, sdf, elemenet_coder, restriction_coder, windowing_strategy):
    self.sdf = sdf
    self.element_coder = elemenet_coder
    self.restriction_coder = restriction_coder
    self._element_tag = _ValueStateTag('element')
    self._restriction_tag = _ValueStateTag('restriction')
    self.watermark_hold_tag = _ValueStateTag('watermark_hold')
    self._process_element_invoker = None

    # TODO: Do we need a more advanced invoker ?
    self.sdf_invoker = DoFnInvoker.create_invoker(
        DoFnSignature(self.sdf), context=DoFnContext('unused_context'))

  def set_step_context(self, step_context):
    assert isinstance(step_context, DirectStepContext)
    self._step_context = step_context

  def set_process_element_invoker(self, process_element_invoker):
    assert isinstance(process_element_invoker, SplittableProcessElementInvoker)
    self._process_element_invoker = process_element_invoker

  def start_bundle(self):
    super(ProcessFn, self).start_bundle()
    # TODO: support start_bundle() method for SDFs.

  def process(self, element, timestamp=beam.DoFn.TimestampParam,
              window=beam.DoFn.WindowParam, * args, **kwargs):
    if isinstance(element, KeyedWorkItem):
      # must ve a timer firing.
      key = element.encoded_key
    else:
      key, value = element

    state = self._step_context.get_keyed_state(key)
    assert isinstance(state, DirectUnmergedState) # TODO: delete

    element_state = state.get_state(window, self._element_tag)
    is_seed_call = not element_state # Initially element_state is an empty list.

    if not is_seed_call:
      element = state.get_state(window, self._element_tag)
      restriction = state.get_state(window, self._restriction_tag)
      windowed_element = WindowedValue(element, timestamp, [window])
    else:
      element_and_restriction = (
        value.value if isinstance(value, WindowedValue) else value)[0]
      assert isinstance(element_and_restriction, ElementAndRestriction)
      element = element_and_restriction.element
      restriction = element_and_restriction.restriction

      if isinstance(value, WindowedValue):
        windowed_element = WindowedValue(
            element, value.timestamp, value.windows)
      else:
        windowed_element = WindowedValue(element, timestamp, [window])

    tracker = self.sdf_invoker.invoke_new_tracker(restriction)
    assert self._process_element_invoker
    assert isinstance(self._process_element_invoker,
                      SplittableProcessElementInvoker)

    invocation_result = []
    def result_callback(result):
      invocation_result.append(result)

    output_values = self._process_element_invoker.invoke_process_element(
        self.sdf_invoker, windowed_element, tracker,
        result_callback=result_callback)
    for output in output_values:
      yield output

    assert len(invocation_result) == 1
    sdf_result = invocation_result[0]
    assert isinstance(sdf_result, SplittableProcessElementInvoker.Result)

    if not sdf_result.residual_restriction:
      # All work for current resudual and restriction pair is complete.
      state.clear_state(window, self._element_tag)
      state.clear_state(window, self._restriction_tag)
      # TODO: clear hold state
      # self.watermark_hold = (key, WatermarkManager.WATERMARK_POS_INF)
      # self.set_timer = False
      state.add_state(window, self.watermark_hold_tag,
                      WatermarkManager.WATERMARK_POS_INF)
    else:
      state.add_state(window, self._element_tag, element)
      state.add_state(window, self._restriction_tag,
                      sdf_result.residual_restriction)
      state.add_state(window, self.watermark_hold_tag,
                      WatermarkManager.WATERMARK_NEG_INF)
      # TODO: Hold watermark
      # TODO: Use state.set_timer(window, '', TimeDomain.REAL_TIME, time.time())
      # instead of following (setting a boolean and setting an unprocessed bundle
      # in TransformEvaluator) set a timer for (current time + n sec) once that
      # API is available. Following only works for DirectRunner.
      # self.set_timer = True
      # self.element_to_store = (key, windowed_element)
      # self.watermark_hold = (key, WatermarkManager.WATERMARK_NEG_INF)
      state.set_timer(
          window, '', TimeDomain.WATERMARK, WatermarkManager.WATERMARK_NEG_INF)

    process_finish = True

  def finish_bundle(self):
    super(ProcessFn, self).finish_bundle()
    # TODO: support finish_bundle() method for SDFs.


class SplittableProcessElementInvoker(DoFnInvoker):
  """A runner specific utility that used for invoking process() of SDFs."""

  class Result(object):
    def __init__(
        self, residual_restriction=None, continuation=None,
        future_output_watermark=None):
      self.residual_restriction = residual_restriction
      self.continuation = continuation
      self.future_output_watermark = future_output_watermark

  def invoke_process_element(self, invoker, element, tracker, result_callback,
                             *args, **kwargs):
    # Returns a tuple (residual_restriction, future_output_watermark)
    raise NotImplementedError


class OutputAndTimeBoundSplittableProcessElementInvoker(
    SplittableProcessElementInvoker):
  """A SplittableProcessElementInvoker that requsts checkpoints.

  Requests a checkpoint after processing a predefined number of elements or
  after a predefined time is elapsed from the process() invocation.
  """

  def __init__(
      self, max_num_outputs, max_duration):
    self._max_num_outputs = max_num_outputs
    self._max_duration = max_duration
    self._checkpoint_lock = Lock()

  def invoke_process_element(
      self, invoker, element, tracker, result_callback, *args, **kwargs):
    assert isinstance(invoker, DoFnInvoker)
    assert isinstance(tracker, RestrictionTracker)
    checkpointed = []
    residual_restriction = []

    def initiate_checkpoint():
      with self._checkpoint_lock:
        if checkpointed:
          return
      residual_restriction.append(tracker.checkpoint())
      checkpointed.append(object())

    output_processor = _OutputProcessor()
    Timer(self._max_duration, initiate_checkpoint).start()
    invoker.invoke_process(
        element, restriction_tracker=tracker, output_processor=output_processor,
        *args, **kwargs)

    assert output_processor.output_iter is not None
    output_count = 0
    for output in output_processor.output_iter:
      yield output
      output_count += 1
      if self._max_num_outputs and output_count >= self._max_num_outputs:
        initiate_checkpoint()

    tracker.check_done()
    result = (
      SplittableProcessElementInvoker.Result(
          residual_restriction=residual_restriction[0]) if residual_restriction
      else SplittableProcessElementInvoker.Result())
    result_callback(result)


class _OutputProcessor(OutputProcessor):

  def __init__(self):
    self.output_iter = None

  def process_outputs(self, windowed_input_element, output_iter):
    self.output_iter = output_iter
