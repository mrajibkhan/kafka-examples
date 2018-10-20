package com.mrk.kafka.controllers;

import com.mrk.kafka.basic.MessageService;
import com.mrk.kafka.models.Greeting;
import com.mrk.kafka.stream.GreetingsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
@RestController
public class GreetingsController {
    private final GreetingsService greetingsService;
    public GreetingsController(GreetingsService greetingsService) {
        this.greetingsService = greetingsService;
    }

    @Autowired
    private MessageService messageService;

    @GetMapping("/greetings")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void greetings(@RequestParam("message") String message, @RequestParam("name") String name) {
        Greeting greetings = Greeting.builder()
                .msg(message)
                .name(name)
                .build();
        greetingsService.sendGreeting(greetings);
    }

    @GetMapping("/basic")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void runBasicExamples() throws Exception {
        messageService.publishMessage();
    }


}
