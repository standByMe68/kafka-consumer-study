import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumerController {

    public static void main(String[] args) {

        Map<String,Integer> map = new HashMap<>();

        map.put("study",1);

        ConsumerConnector connector = KafkaConsumerConfig.getKafkaConsumer();

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = connector.createMessageStreams(map);

        List<KafkaStream<byte[], byte[]>> study = messageStreams.get("study");

        System.out.println(messageStreams.size());

        for (KafkaStream stream : study){
            ConsumerIterator iterator = stream.iterator();

            while (iterator.hasNext()){

                MessageAndMetadata next = iterator.next();

                int count = 1;

                byte[] message =(byte[]) next.message();

                String s = new String(message);

                System.out.println(s);

                count++;

                connector.commitOffsets();

            }
        }

    }

}
