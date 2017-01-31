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

"""
User-facing classes for Metrics API.

The classes in this file allow users to define and use metrics to be collected
and displayed as part of their pipeline execution.

- Metrics - This class lets pipeline and transform writers create and access
    metric objects such as counters, distributions, etc.
"""
from __future__ import unicode_literals
from builtins import object
import inspect

from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metricbase import Counter, Distribution
from apache_beam.metrics.metricbase import MetricName


try:
  basestring
  def isstring(s):
    return isinstance(s, basestring)
except NameError:
  def isstring(s):
    return isinstance(s,str)

class Metrics(object):
  """Lets users create/access metric objects during pipeline execution.
  """
  @staticmethod
  def get_namespace(namespace):
    if inspect.isclass(namespace):
      return '{}.{}'.format(namespace.__module__, namespace.__name__)
    elif isstring(namespace):
      return namespace
    else:
      raise ValueError('Unknown namespace type')

  @staticmethod
  def counter(namespace, name):
    """Obtains or creates a Counter metric.

    Args:
      namespace: A class or string that gives the namespace to a metric
      name: A string that gives a unique name to a metric

    Returns:
      A Counter object.
    """
    namespace = Metrics.get_namespace(namespace)
    return Metrics.DelegatingCounter(MetricName(namespace, name))

  @staticmethod
  def distribution(namespace, name):
    """Obtains or creates a Distribution metric.

    Distribution metrics are restricted to integer-only distributions.

    Args:
      namespace: A class or string that gives the namespace to a metric
      name: A string that gives a unique name to a metric

    Returns:
      A Distribution object.
    """
    namespace = Metrics.get_namespace(namespace)
    return Metrics.DelegatingDistribution(MetricName(namespace, name))

  class DelegatingCounter(Counter):
    def __init__(self, metric_name):
      self.metric_name = metric_name

    def inc(self, n=1):
      container = MetricsEnvironment.current_container()
      if container is not None:
        container.get_counter(self.metric_name).inc(n)

  class DelegatingDistribution(Distribution):
    def __init__(self, metric_name):
      self.metric_name = metric_name

    def update(self, value):
      container = MetricsEnvironment.current_container()
      if container is not None:
        container.get_distribution(self.metric_name).update(value)


class MetricResults(object):
  @staticmethod
  def matches(filter, metric_key):
    if filter is None:
      return True

    if (metric_key.step in filter.steps and
        metric_key.metric.namespace in filter.namespaces and
        metric_key.metric.name in filter.names):
      return True
    else:
      return False

  def query(self, filter):
    raise NotImplementedError


class MetricsFilter(object):
  """Simple object to filter metrics results.

  If filters by matching a result's step-namespace-name with three internal
  sets. No execution/matching logic is added to this object, so that it may
  be used to construct arguments as an RPC request. It is left for runners
  to implement matching logic by themselves.
  """
  def __init__(self):
    self._names = set()
    self._namespaces = set()
    self._steps = set()

  @property
  def steps(self):
    return frozenset(self._steps)

  @property
  def names(self):
    return frozenset(self._names)

  @property
  def namespaces(self):
    return frozenset(self._namespaces)

  def with_name(self, name):
    return self.with_names([name])

  def with_names(self, names):
    if isstring(names):
      raise ValueError('Names must be an iterable, not a string')

    self._steps.update(names)
    return self

  def with_namespace(self, namespace):
    return self.with_namespaces([namespace])

  def with_namespaces(self, namespaces):
    if isstring(namespaces):
      raise ValueError('Namespaces must be an iterable, not a string')

    self._namespaces.update([Metrics.get_namespace(ns) for ns in namespaces])
    return self

  def with_step(self, step):
    return self.with_steps([step])

  def with_steps(self, steps):
    if isstring(namespaces):
      raise ValueError('Steps must be an iterable, not a string')

    self._steps.update(steps)
    return self
