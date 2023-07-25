package org.acme;

import com.acme.MyRecordOuterClass;
import com.acme.Other;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducerRecord;
import jakarta.inject.Inject;
import org.acme.config.KafkaResource;
import org.acme.config.SchemaRegistryResource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@QuarkusTest
@QuarkusTestResource(KafkaResource.class)
@QuarkusTestResource(SchemaRegistryResource.class)
public class KafkaProtobufTest {

    Logger log = Logger.getLogger(KafkaProtobufTest.class);

    @Inject
    Vertx vertx;

    @ConfigProperty(name = "kafka.bootstrap-servers")
    String bootstrapServer;

    @ConfigProperty(name = "kafka.schema-registry-url")
    String schemaRegistryUrl;

    @Test
    public void stringKafkaTest() throws InterruptedException {

        String topic = "test-topic-string";


        // produce
        Properties propsProducer = new Properties();

        propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        propsProducer.put(ProducerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        propsProducer.put("schema.registry.url", schemaRegistryUrl);

        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, toMap(propsProducer));

        produceString(topic, producer);


        // consume
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-" + UUID.randomUUID());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, toMap(props));
        AtomicInteger messageCountAssign = new AtomicInteger();

        consumer.handler(record -> {
            log.info("Processing key=" + record.key() +
                    ",value=" + record.value() +
                    ",partition=" + record.partition() +
                    ",offset=" + record.offset());
            messageCountAssign.getAndIncrement();
        });

        consumer.exceptionHandler(
                error -> {
                    log.error("Error with kafka consumer: ", error);
                });

        final TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(partition).subscribe().with(a -> {
            log.info("Subscribed");
        });

        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topic));


        Thread.sleep(100);
        produceString(topic, producer);
        Thread.sleep(2000);

        producer.close().await().indefinitely();
        consumer.close().await().indefinitely();

        ConsumerRecords<String, String> records= kafkaConsumer.poll(Duration.ofSeconds(10));
        Assertions.assertEquals(2, records.count());


        Assertions.assertEquals(2, messageCountAssign.get());

    }


    @Test
    public void protoKafkaTest() throws InterruptedException {

        String topic = "test-topic";


        // produce
        Properties propsProducer = new Properties();

        propsProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        propsProducer.put(ProducerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        propsProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        propsProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");

        propsProducer.put("schema.registry.url", schemaRegistryUrl);

        KafkaProducer<String, MyRecordOuterClass.MyRecord> producer = KafkaProducer.create(vertx, toMap(propsProducer));

        MyRecordOuterClass.MyRecord myrecord = produceProto(topic, producer);


        // consume
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-" + UUID.randomUUID());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer");
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, "com.acme.MyRecordOuterClass$MyRecord");
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, MyRecordOuterClass.MyRecord> consumer = KafkaConsumer.create(vertx, toMap(props));
        AtomicInteger messageCountAssign = new AtomicInteger();

        consumer.handler(record -> {
            log.info("Processing key=" + record.key() +
                    ",value=" + record.value() +
                    ",partition=" + record.partition() +
                    ",offset=" + record.offset());
            messageCountAssign.getAndIncrement();
        });

        consumer.exceptionHandler(
                error -> {
                    log.error("Error with kafka consumer: ", error);
                });

        final TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(partition).subscribe().with(a -> {
            log.info("Subscribed");
        });

        Thread.sleep(100);
        produceProto(topic, producer);
        Thread.sleep(1000);

        producer.close().await().indefinitely();
        consumer.close().await().indefinitely();


        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topic));

        ConsumerRecords<String, String> records= kafkaConsumer.poll(Duration.ofSeconds(10));
        Assertions.assertEquals(2, records.count());


        Assertions.assertEquals(2, messageCountAssign.get());

    }

    private static void produceString(String topic, KafkaProducer<String, String> producer) {
        producer.send(KafkaProducerRecord.create(topic, "key", "test")).await().indefinitely();
    }

    private static MyRecordOuterClass.MyRecord produceProto(String topic, KafkaProducer<String, MyRecordOuterClass.MyRecord> producer) {
        Other.OtherRecord otherRecord = Other.OtherRecord.newBuilder()
                .setOtherId(123).build();
        MyRecordOuterClass.MyRecord myrecord = MyRecordOuterClass.MyRecord.newBuilder()
                .setF1("value1").setF2(otherRecord).build();


        producer.send(KafkaProducerRecord.create(topic, "key", myrecord)).await().indefinitely();
        return myrecord;
    }

    private Map<String, String> toMap(Properties props) {
        return props.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }
}
