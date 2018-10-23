package com.mrk.kafka.controllers;


import com.mrk.kafka.models.Greeting;
import com.mrk.kafka.reactor.AbstractScenario;
import com.mrk.kafka.reactor.CommittableSource;
import com.mrk.kafka.reactor.KafkaSink;
import com.mrk.kafka.reactor.KafkaSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

@RequestMapping(path = "/sse")
@RestController
public class SSEController {

//    @Autowired
//    KafkaReceiver<String, Greeting> kafkaReceiver;

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    private ConnectableFlux<ServerSentEvent<Greeting>> eventPublisher;

    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Mono<String> getSimpleFlux() throws Exception {
//        EmitterProcessor processor = kafkaReceiver.receive()
//                .map( it -> {
//                    it.receiverOffset().acknowledge();
//                    return it;
//                }).subscribeWith(EmitterProcessor.create(false));
//        return processor.take(1);

//        Flux<ReceiverRecord<String,Object>> kafkaFlux = kafkaReceiver.receive();
//
//        return kafkaFlux.log().doOnNext( r -> r.receiverOffset().acknowledge() )
//                .map(ReceiverRecord::value);

//        eventPublisher = kafkaReceiver.receive()
//                .map(consumerRecord -> ServerSentEvent.builder(consumerRecord.value()).build())
//                .publish();
//
//        // subscribes to the KafkaReceiver -> starts consumption (without observers attached)
//        eventPublisher.connect();
//
//        return ServerResponse.status(HttpStatus.OK).body(BodyInserters.fromServerSentEvents(eventPublisher));

//        CommittableSource source = new CommittableSource();
//
//        AbstractScenario sinkScenario = new KafkaSink(bootstrapAddress, "reactor-topic");
//        sinkScenario.source(source);
//        sinkScenario.runScenario();
//
//        AbstractScenario sampleScenario = new KafkaSource(bootstrapAddress, "reactor-topic");
//        sampleScenario.runScenario();

        return Mono.empty();

    }
}
