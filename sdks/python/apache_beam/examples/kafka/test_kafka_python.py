import time

msg_count = 1000000
msg_size = 1024 * 10
msg_payload = ('kafkatest123' * 1024*100).encode()[:msg_size]
print('Message payload is %s' % msg_payload)
print('Length of message payload is %d' % len(msg_payload))

runs=5

from kafka import KafkaProducer
from kafka import KafkaConsumer

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
  producer = KafkaProducer(bootstrap_servers='localhost:9092')

  producer_start = time.time()
  topic = 'python-kafka-topic'
  for i in range(msg_count):
    producer.send(topic, msg_payload)

  producer.flush()  # clear all local buffers and produce pending messages

  return time.time() - producer_start

def test_consumer():
  topic = 'python-kafka-topic'

  consumer = KafkaConsumer(
      bootstrap_servers='localhost:9092',
      auto_offset_reset='earliest',  # start at earliest topic
      group_id=None  # do no offest commit
  )
  msg_consumed_count = 0

  consumer_start = time.time()
  consumer.subscribe([topic])
  for msg in consumer:
    msg_consumed_count += 1

    if msg_consumed_count >= msg_count:
      break

  consumer_timing = time.time() - consumer_start
  consumer.close()
  return consumer_timing

def test_split():
  pass


print('\n\nRunning kafka-python producer experiment...')

producer_timing_records = []
for _ in range(runs):
  producer_timing_records.append(test_producer())
calculate_thoughput(producer_timing_records, msg_count, msg_size)

print('\n\nRunning kafka-python consumer experiment...')

consumer_timing_records = []
for _ in range(runs):
  consumer_timing_records.append(test_consumer())
calculate_thoughput(consumer_timing_records, msg_count, msg_size)