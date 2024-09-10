#!/bin/bash

set -ex

roachprod sync

roachprod install wenyi-drt-kafka docker
roachprod ssh wenyi-drt-kafka 'mkdir -p ~/kafka'
roachprod ssh wenyi-drt-kafka 'cd kafka && docker compose down' || true
roachprod ssh wenyi-drt-kafka 'rm -rf /mnt/data1/kafka/'

for NUM in 1 2 3
do
 cat > docker-compose.yml <<!
version: '2'
services:
  kafka:
    image: confluentinc/cp-kafka:latest
    network_mode: host
    environment:
      KAFKA_BROKER_ID: ${NUM}
      KAFKA_ZOOKEEPER_CONNECT: wenyi-drt-kafka-0001.roachprod.crdb.io:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://wenyi-drt-kafka-000${NUM}.roachprod.crdb.io:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3

      # i'm not sure why these settings are not respected
      KAFKA_LOG_RETENTION_MS: 100
      KAFKA_LOG_RETENTION_BYTES: 1048576
      KAFKA_LOG_SEGMENT_BYTES: 1048576

      KAFKA_LOG_DIRS: /data/kafka
    volumes:
       - /mnt/data1:/data
    restart: always
!

 if [ $NUM -eq 1 ] ; then
   cat >> docker-compose.yml <<!
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    network_mode: "host"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    restart: always
!
 fi
 roachprod put wenyi-drt-kafka:$NUM docker-compose.yml 'kafka/docker-compose.yml'
 roachprod ssh wenyi-drt-kafka:$NUM "cd kafka && docker compose up -d"
done

# set retention config

cat > /tmp/set-configs.sh <<'EOF'
#!/bin/bash
set -ex
sudo apt-get install -y jq
cnt=$(docker container ls --format json | jq .ID -r)
echo "container id is $cnt"
docker container exec $cnt bash -c 'for n in 1 2 3; do kafka-configs --bootstrap-server localhost:9092 --entity-type brokers --entity-name $n --alter --add-config 'log.retention.ms=100,log.retention.bytes=1048576' ; done'
EOF

roachprod put wenyi-drt-kafka:2 /tmp/set-configs.sh /tmp/set-configs.sh
roachprod ssh wenyi-drt-kafka:2 'chmod +x /tmp/set-configs.sh && /tmp/set-configs.sh'
