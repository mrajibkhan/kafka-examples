package com.mrk.kafka.models;


import lombok.*;

@NoArgsConstructor @AllArgsConstructor
@Getter @Setter @ToString @Builder
public class Greeting {

    private String msg;
    private String name;

}
