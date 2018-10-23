package com.mrk.kafka.reactor;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

/**
 * This sample demonstrates the use of Kafka as a sink when messages are transferred from
 * an external source to a Kafka topic. Unlimited (very large) blocking time and retries
 * are used to handle broker failures. Source records are committed when sends succeed.
 *
 */
public class KafkaSink extends AbstractScenario {
    private final String topic;

    public KafkaSink(String bootstrapServers, String topic) {
        super(bootstrapServers);
        this.topic = topic;
    }
    public Flux<?> flux() {
        SenderOptions<Integer, Person> senderOptions = senderOptions()
                .producerProperty(ProducerConfig.ACKS_CONFIG, "all")
                .producerProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE)
                .producerProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        Flux<Person> srcFlux = source().flux();
        return sender(senderOptions)
                .send(srcFlux.map(p -> SenderRecord.create(new ProducerRecord<>(topic, p.id(), p), p.id())))
                .doOnError(e -> log.error("Send failed, terminating.", e))
                .doOnNext(r -> {
                    int id = r.correlationMetadata();
                    log.trace("Successfully stored person with id {} in Kafka", id);
                    source.commit(id);
                })
                .doOnCancel(() -> close());
    }
}

