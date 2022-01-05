package consumer;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ultiliities.Operation.Calculator;
import ultiliities.validation.Validation;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumer {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(consumer.class.getName());
        String bootstrapServers="127.0.0.1:9092";
        String grp_id="first_app";
        String topic="my_first";
        //Creating consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,grp_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //creating consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String,String>(properties);
        //Subscribing
        consumer.subscribe(Arrays.asList(topic));
        //polling
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record: records){
                JsonObject param= new JsonObject(record.value());
                if (Validation.isNumeric(String.valueOf(param.getValue("a"))) && Validation.isNumeric(String.valueOf(param.getValue("b")))) {
                    if (Validation.validateOpe(param.getDouble("b"),param.getString("ope")) == 1) {
                        double m = Calculator.operation(param.getDouble("a"), param.getDouble("b"), param.getString("ope"));
                        System.out.println("{"+param.getDouble("a")+", "+param.getDouble("b")+", "+ param.getString("ope")+"}");
                        System.out.println("Result: "+m);
                        logger.info("Key: "+ record.key() + ", Value:" +record.value());
                        logger.info("Partition:" + record.partition()+",Offset:"+record.offset());
                    } else if (Validation.validateOpe(param.getDouble("b"), param.getString("ope")) == 0) {
                        System.out.println("You can't put 0 under the denominator");
                    } else if (Validation.validateOpe(param.getDouble("b"), param.getString("ope")) == -1) {
                        System.out.println("Please enter the right operation (+, -, x, :)");
                    }
                } else {
                    System.out.println("Error");
                }

            }


        }
    }
}
