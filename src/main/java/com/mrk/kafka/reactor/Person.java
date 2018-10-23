package com.mrk.kafka.reactor;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

public class Person {

        private final int id;
        private final String firstName;
        private final String lastName;
        private String email;
        public Person(int id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }
        public int id() {
            return id;
        }
        public String firstName() {
            return firstName;
        }
        public String lastName() {
            return lastName;
        }
        public void email(String email) {
            this.email = email;
        }
        public String email() {
            return email == null ? "" : email;
        }
        public Person upperCase() {
            return new Person(id, firstName.toUpperCase(Locale.ROOT), lastName.toUpperCase(Locale.ROOT));
        }
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Person))
                return false;

            Person p = (Person) other;

            if (id != p.id)
                return false;
            return stringEquals(firstName, p.firstName) &&
                    stringEquals(lastName, p.lastName) &&
                    stringEquals(email, p.email);
        }

        @Override
        public int hashCode() {
            int result = Integer.hashCode(id);
            result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
            result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
            return result;
        }
        public String toString() {
            return "Person{" +
                    "id='" + id + '\'' +
                    ", firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    ", email='" + email + '\'' +
                    '}';
        }
        private boolean stringEquals(String str1, String str2) {
            return str1 == null ? str2 == null : str1.equals(str2);
        }
    }


