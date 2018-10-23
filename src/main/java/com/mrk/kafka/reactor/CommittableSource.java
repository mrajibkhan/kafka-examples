package com.mrk.kafka.reactor;

import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

public class CommittableSource {
    private List<Person> sourceList = new ArrayList<>();
    public CommittableSource() {
        sourceList.add(new Person(1, "John", "Doe"));
        sourceList.add(new Person(1, "Ada", "Lovelace"));
    }
    public CommittableSource(List<Person> list) {
        sourceList.addAll(list);
    }
    Flux<Person> flux() {
        return Flux.fromIterable(sourceList);
    }

    void commit(int id) {
        log.trace("Committing {}", id);
    }
}
