import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class Test {
	public static void main(String[] args) {
        long events = Long.parseLong("100000");
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "app101.dev.la1.vcinv.net:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = nEvents + ",www.example.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
               producer.send(data);
        }
        producer.close();
    }
}
