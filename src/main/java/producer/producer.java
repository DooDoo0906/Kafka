package producer;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class producer {
    public static void main(String[] args) {
        String boostrapServers="127.0.0.1:9092";
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> first_producer = new KafkaProducer<String, String>(properties);
        JsonObject object= new JsonObject();
        object.put("a",1);
        object.put("b",2);
        object.put("ope","+");
        ProducerRecord<String, String> record=new ProducerRecord<String, String>("my_first", object.encodePrettily());
        first_producer.send(record);
        first_producer.flush();
        first_producer.close();
    }
}
