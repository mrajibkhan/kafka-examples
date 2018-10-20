package com.mrk.kafka.stream;

import com.mrk.kafka.models.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
@Component
@Slf4j
public class GreetingsListener {
    @StreamListener(GreetingsStream.INPUT)
    public void handleGreetings(@Payload Greeting greetings) {
        log.info("Received greetings: {}", greetings);
    }
}
