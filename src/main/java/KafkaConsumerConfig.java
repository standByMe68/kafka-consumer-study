import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.Properties;

public class KafkaConsumerConfig {


    private static  Properties getKafkaConsumerppProperties(){

        Properties kafkaConsumerppProperties = new Properties();
        kafkaConsumerppProperties.put("zookeeper.connect", "192.168.2.100:2181");
        kafkaConsumerppProperties.put("zookeeper.session.timeout.ms", "50000");
        kafkaConsumerppProperties.put("zookeeper.connection.timeout.ms", "100000");
        kafkaConsumerppProperties.put("metadata.broker.list", "192.168.2.100:9092");
        kafkaConsumerppProperties.put("rebalance.backoff.ms", "2000");
        kafkaConsumerppProperties.put("rebalance.max.retries", "10");
        kafkaConsumerppProperties.put("group.id", "client_" + 1);
        kafkaConsumerppProperties.put("auto.offset.reset", "largest");
        kafkaConsumerppProperties.put("enable.auto.commit", "false");
        kafkaConsumerppProperties.put("max.poll.records", 1);
        kafkaConsumerppProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerppProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return kafkaConsumerppProperties;
    }


    public static ConsumerConnector getKafkaConsumer(){

        Properties kafkaConsumerppProperties = getKafkaConsumerppProperties();

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaConsumerppProperties));
    }



}
