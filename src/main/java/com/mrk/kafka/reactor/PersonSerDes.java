package com.mrk.kafka.reactor;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PersonSerDes implements Serializer<Person>, Deserializer<Person> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Person person) {
        byte[] firstName = person.firstName().getBytes(StandardCharsets.UTF_8);
        byte[] lastName = person.lastName().getBytes(StandardCharsets.UTF_8);
        byte[] email = person.email().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + firstName.length + 4 + lastName.length + 4 + email.length);
        buffer.putInt(person.id());
        buffer.putInt(firstName.length);
        buffer.put(firstName);
        buffer.putInt(lastName.length);
        buffer.put(lastName);
        buffer.putInt(email.length);
        buffer.put(email);
        return buffer.array();
    }

    @Override
    public Person deserialize(String topic, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int id = buffer.getInt();
        byte[] first = new byte[buffer.getInt()];
        buffer.get(first);
        String firstName = new String(first, StandardCharsets.UTF_8);
        byte[] last = new byte[buffer.getInt()];
        buffer.get(last);
        String lastName = new String(last, StandardCharsets.UTF_8);
        Person person = new Person(id, firstName, lastName);
        byte[] email = new byte[buffer.getInt()];
        if (email.length > 0) {
            buffer.get(email);
            person.email(new String(email, StandardCharsets.UTF_8));
        }
        return person;
    }

    @Override
    public void close() {
    }
}
