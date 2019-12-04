import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerAS {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerAS.class);

        // Definimos las properties

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

      //  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ooo");

      //  properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Creamos el consumidor

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

      //  kafkaConsumer.subscribe(Arrays.asList("primer-topic"));

      TopicPartition topicPartition = new TopicPartition("primer-topic", 0);
      kafkaConsumer.assign(
          Arrays.asList(topicPartition)
      );

      long offset = 22L;

      kafkaConsumer.seek(topicPartition, offset);

   //     Leemos
        CountDownLatch latch = new CountDownLatch(1);
        final Runnable hilo = new ConsumerThread(latch, kafkaConsumer);

        Thread thread = new Thread(hilo);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("parando el sistema");
                ((ConsumerThread) hilo).shutdown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    logger.info("terminado");
                }
            }
        });

    }

}