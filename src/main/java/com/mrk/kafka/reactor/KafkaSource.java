package com.mrk.kafka.reactor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Duration;
import java.util.Collections;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

/**
 * This sample demonstrates the use of Kafka as a source when messages are transferred from
 * a Kafka topic to an external sink. Kafka offsets are committed when records are successfully
 * transferred. Unlimited retries on the source Kafka Flux ensure that the Kafka consumer is
 * restarted if there are any exceptions while processing records.
 */
public  class KafkaSource extends AbstractScenario {
    private final String topic;
    private final Scheduler scheduler;

    public KafkaSource(String bootstrapServers, String topic) {
        super(bootstrapServers);
        this.topic = topic;
        this.scheduler = Schedulers.newSingle("sample", true);
    }
    public Flux<?> flux() {
        return KafkaReceiver.create(receiverOptions(Collections.singletonList(topic)).commitInterval(Duration.ZERO))
                .receive()
                .publishOn(scheduler)
                .concatMap(m -> storeInDB(m.value())
                        .thenEmpty(m.receiverOffset().commit()))
                .retry()
                .doOnCancel(() -> close());
    }
    public Mono<Void> storeInDB(Person person) {
        log.info("Successfully processed person with id {} from Kafka", person.id());
        return Mono.empty();
    }
    public void close() {
        super.close();
        scheduler.dispose();
    }

}