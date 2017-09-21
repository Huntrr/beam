import apache_beam as beam

from apache_beam.runners.sdf_common import ProcessKeyedElements, \
  GBKIntoKeyedWorkItems

from apache_beam.pipeline import AppliedPTransform, PTransformOverride


class ProcessKeyedElementsOverride(PTransformOverride):

  @staticmethod
  def _process_keyed_elements_matcher(applied_ptransform):
    assert isinstance(applied_ptransform, AppliedPTransform)

    transform = applied_ptransform.transform
    return isinstance(transform, ProcessKeyedElements)

  def get_matcher(self):
    return ProcessKeyedElementsOverride._process_keyed_elements_matcher

  def get_replacement_transform(self, ptransform):
    assert isinstance(ptransform, ProcessKeyedElements)

    return


class DirectGBKIntoKeyedWorkItemsOverride(PTransformOverride):

  def get_matcher(self):
    return (lambda applied_pt : isinstance(applied_pt.transform, GBKIntoKeyedWorkItems))

  def get_replacement_transform(self, ptransform):
    return beam.core._GroupByKeyOnly()



