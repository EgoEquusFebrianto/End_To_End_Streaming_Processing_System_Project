package kudadiri.DataEngineer.portofolio.config;

import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;

public class KafkaConfig {
    public static Properties getProducerConfig(String servers, String clientId) {
        String serializer = StringSerializer.class.getName();
        String avroSerializer = AvroSerializer.class.getName();
        Properties props = new Properties();
//        props.put("client.id", clientId);
        props.put("bootstrap.servers", servers);
        props.put("key.serializer", serializer);
        props.put("value.serializer", avroSerializer);

        // Throughput + Skalabilitas
        props.put("batch_size", 32768); // 32KB
        props.put("linger.ms", 5);
        props.put("compression.type", "lz4");

        // Buffer
        props.put("buffer.memory", 67108864); // 64 MB

        // Partitioning
        props.put("partitioner.class", RoundRobinPartitioner.class.getName());

//        // SSL Config
//        props.put("security.protocol", "SSL");
//        props.put("ssl.truststore.location", "secret/ssl/kafka.truststore.jks");
//        props.put("ssl.truststore.password", "password");

        return props;
    }
}
