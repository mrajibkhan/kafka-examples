package com.mrk.kafka.stream;

import com.mrk.kafka.models.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;
@Service
@Slf4j
public class GreetingsService {
    private final GreetingsStream greetingsStream;
    public GreetingsService(GreetingsStream greetingsStream) {
        this.greetingsStream = greetingsStream;
    }
    public void sendGreeting(final Greeting greetings) {
        log.info("Sending greetings {}", greetings);
        MessageChannel messageChannel = greetingsStream.outboundGreetings();
        messageChannel.send(MessageBuilder
                .withPayload(greetings)
                .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
                .build());
    }
}
