package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Locale;

public class GreetingsTopology {
  public static final String GREETINGS = "greetings";
  public static final String GREETINGS_UPPERCASE = "greetings_uppercase";

  public static Topology buildTopology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    var greetingsStream =
        streamsBuilder.stream(
            GREETINGS, Consumed.with(Serdes.String(), Serdes.String())); // consumer api

    greetingsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

    var greetingsUppercaseStream =
        greetingsStream.mapValues((readOnlyKey, value) -> value.toUpperCase(Locale.ROOT));

    greetingsUppercaseStream.print(
        Printed.<String, String>toSysOut().withLabel("greetingsUppercaseStream"));

    greetingsUppercaseStream.to(
        GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String())); // producer api

    return streamsBuilder.build();
  }
}
