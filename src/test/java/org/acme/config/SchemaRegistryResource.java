package org.acme.config;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.Map;

public class SchemaRegistryResource implements QuarkusTestResourceLifecycleManager {

  public static SchemaRegistryContainer schemaRegistry =
      new SchemaRegistryContainer()
          .withKafka(KafkaResource.kafka)
          .withNetwork(NetworkConfig.NETWORK);

  @Override
  public Map<String, String> start() {
    schemaRegistry.start();
    return Map.of("kafka.schema-registry-url", schemaRegistry.getSchemaRegistryUrl());
  }

  @Override
  public void stop() {
    schemaRegistry.stop();
  }

  @Override
  public int order() {
    return 2;
  }
}
