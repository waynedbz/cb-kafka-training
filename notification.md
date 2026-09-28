# Notification producer, consumer, topic/s and event/s

## Topic configs
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic notification.events \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=snappy \
  --config max.message.bytes=1048576

  ### Consumer configs

kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic payment.events \
  --property parse.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group notification-dispatch-group \
  --property max.poll.records=500 \
  --property session.timeout.ms=10000 \
  --property heartbeat.interval.ms=3000 \
  --property auto.offset.reset=earliest \
  --property enable.auto.commit=false \
  --property auto.commit.interval.ms=5000 \
  --property max.poll.interval.ms=300000 \
  --property fetch.min.bytes=1 \
  --property fetch.max.wait.ms=500 \
  --property max.partition.fetch.bytes=1048576 \
  --property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor


### Producer configs

kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic notification.events \
  --property parse.key=true \
  --property key.separator=: \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property acks=all \
  --property retries=2147483647 \
  --property max.in.flight.requests.per.connection=5 \
  --property enable.idempotence=true \
  --property compression.type=snappy \
  --property linger.ms=5 \
  --property batch.size=32768 \
  --property delivery.timeout.ms=120000 \
  --property request.timeout.ms=30000