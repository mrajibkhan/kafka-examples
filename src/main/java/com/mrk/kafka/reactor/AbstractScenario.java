package com.mrk.kafka.reactor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.*;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

public abstract class AbstractScenario {
    String bootstrapServers = "";
    String groupId = "sample-group";
    CommittableSource source;
    KafkaSender<Integer, Person> sender;
    List<Disposable> disposables = new ArrayList<>();

    AbstractScenario(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
    public abstract Flux<?> flux();

    public void runScenario() throws InterruptedException {
        flux().blockLast();
        //flux().subscribe();
        close();
    }

    public void close() {
        if (sender != null)
            sender.close();
        for (Disposable disposable : disposables)
            disposable.dispose();
    }

    public SenderOptions<Integer, Person> senderOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-reactor-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerDes.class);
        return SenderOptions.create(props);
    }

    public KafkaSender<Integer, Person> sender(SenderOptions<Integer, Person> senderOptions) {
        sender = KafkaSender.create(senderOptions);
        return sender;
    }

    public ReceiverOptions<Integer, Person> receiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-reactor-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PersonSerDes.class);
        return ReceiverOptions.<Integer, Person>create(props);
    }

    public ReceiverOptions<Integer, Person> receiverOptions(Collection<String> topics) {
        return receiverOptions()
                .addAssignListener(p -> log.info("Group {} partitions assigned {}", groupId, p))
                .addRevokeListener(p -> log.info("Group {} partitions assigned {}", groupId, p))
                .subscription(topics);
    }

    public void source(CommittableSource source) {
        this.source = source;
    }

    public CommittableSource source() {
        return source;
    }
}
