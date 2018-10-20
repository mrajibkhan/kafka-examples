package com.mrk.kafka;

import com.mrk.kafka.basic.MessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class KafkaApplication {

	@Autowired
	static MessageService messageService;

	public static void main(String[] args) throws Exception {

		ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);

	}

}