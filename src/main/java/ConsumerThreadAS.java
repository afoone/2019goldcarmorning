import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThreadAS implements Runnable {

    private CountDownLatch latch = null;
    private KafkaConsumer kafkaConsumer;

    public ConsumerThreadAS(CountDownLatch latch, KafkaConsumer kafkaConsumer) {
        this.latch = latch;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void run() {

        Logger logger = LoggerFactory.getLogger(ConsumerThreadAS.class);
      try{
             while (true) {
            ConsumerRecords<String, String> consumerRecords = 
            kafkaConsumer.poll(Duration.ofMillis(100));
    
            for (ConsumerRecord<String,String> 
            consumerRecord 
            : consumerRecords) {
                logger.info("key  " + consumerRecord.key());
                logger.info("value  " + consumerRecord.value());
                logger.info("offset  " + consumerRecord.offset());
                logger.info("partition  " + consumerRecord.partition());
            }
        }}
        catch (WakeupException e) {
            logger.error("recibida señal de shutdown",
             e);
        } finally {
            // cerrar el consumidor
            kafkaConsumer.close();
            // comunicarle al main que hemos terminado con el 
            // consumidor
            latch.countDown();
        }
    }

    public void shutdown() {
        kafkaConsumer.wakeup();
    }



}