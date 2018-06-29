from pykafka import KafkaClient
import time

msg_count = 1000000
msg_size = 1024 * 10
msg_payload = ('kafkatest123' * 1024*100).encode()[:msg_size]
print('Message payload is %s' % msg_payload)
print('Length of message payload is %d' % len(msg_payload))

runs=5


def calculate_thoughput(timing_records, n_messages, msg_size):

  mbs_per_sec = []
  msgs_per_sec = []

  for timing in timing_records:
    mbs_per_sec.append((msg_size * n_messages) / timing / (1024 * 1024))
    msgs_per_sec.append(n_messages / timing)
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))

  print("{0:.2f} MB/s".format(sum(mbs_per_sec) / len(mbs_per_sec)))
  print("{0:.2f} Msgs/s".format(sum(msgs_per_sec) / len(msgs_per_sec)))


def test_producer():
  client = KafkaClient(hosts='localhost:9092')
  topic = client.topics[b'pykafka-test-topic']
  producer = topic.get_producer(use_rdkafka=False)

  produce_start = time.time()
  for i in range(msg_count):
    # Start producing
    producer.produce(msg_payload)

  producer.stop()  # Will flush background queue

  return time.time() - produce_start

def test_consumer():
  client = KafkaClient(hosts='localhost:9092')
  topic = client.topics[b'pykafka-test-topic']

  msg_consumed_count = 0

  consumer_start = time.time()
  # Consumer starts polling messages in background thread, need to start timer here
  consumer = topic.get_simple_consumer(use_rdkafka=False)

  while True:
    msg = consumer.consume()
    if msg:
      msg_consumed_count += 1

    if msg_consumed_count >= msg_count:
      break

  consumer_timing = time.time() - consumer_start
  consumer.stop()
  return consumer_timing

def test_split():
  client = KafkaClient(hosts='localhost:9092')
  topic = client.topics[b'pykafka-test-topic']

  msg_consumed_count = 0

  consumer_start = time.time()
  # Consumer starts polling messages in background thread, need to start timer here
  consumer = topic.get_simple_consumer(use_rdkafka=False)

  topic.get_simple_consumer()


print('\n\nRunning pykafka producer experiment...')

producer_timing_records = []
for _ in range(runs):
  producer_timing_records.append(test_producer())
calculate_thoughput(producer_timing_records, msg_count, msg_size)

print('\n\nRunning pykafka consumer experiment...')

consumer_timing_records = []
for _ in range(runs):
  consumer_timing_records.append(test_consumer())
calculate_thoughput(consumer_timing_records, msg_count, msg_size)