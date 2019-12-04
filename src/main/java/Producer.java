import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Producer.class);
        logger.info("hola mundo");

        // Crear las properties del producer
        Properties properties = new Properties();

        // bootstrap server es el servidor kafka a conectar
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // serializadores
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Crear el producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // Enviar datos
        ProducerRecord<String, String> producerRecord= null;
        for (int i = 0; i < 10; i++) {
            String mensaje = "mensaje "+i;
             producerRecord = new ProducerRecord<String, String>("primer-topic", "mykey",
                    mensaje);
            logger.info(mensaje);
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // Ejecuta lo que corresponda
                    if (exception == null) {
                    
                        logger.info("enviado correctamente mensaje");
                        logger.info("topic " + metadata.topic());
                        logger.info("offset " + metadata.offset());
                        logger.info("partition " + metadata.partition());
                    } else {
                        logger.error(exception.getLocalizedMessage(), exception);
                    }
                }
            });
        }

        kafkaProducer.flush();

    

        kafkaProducer.close();

    }
}
