import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class Producer {
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String TOPIC = "finnhub-stocks";

    private static WebSocketClient getWebSocketClient(String FINNHUB_TOKEN, ObjectMapper mapper,
                                                      KafkaProducer<Object, Object> producer) throws URISyntaxException
    {
        String url = "wss://ws.finnhub.io?token=" + FINNHUB_TOKEN;
        return new WebSocketClient(new URI(url)) {
            @Override
            public void onOpen(ServerHandshake handshakedata) {
                System.out.println("Connected to Finnhub");
                send("{\"type\":\"subscribe\",\"symbol\":\"NVDA\"}");
            }

            @Override
            public void onMessage(String message) {
                try {
                    mapper.readTree(message);
                    producer.send(new ProducerRecord<>(TOPIC, null, message));
                    System.out.println("Sent to Kafka: " + message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                System.out.println("WebSocket closed: " + reason);
            }

            @Override
            public void onError(Exception ex) {
                ex.printStackTrace();
            }
        };
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            try (InputStream input = Producer.class.getClassLoader().getResourceAsStream("config.properties")) {
                properties.load(input);
            }
            String FINNHUB_TOKEN = properties.getProperty("FINNHUB_TOKEN");
            ObjectMapper mapper = new ObjectMapper();
            KafkaProducer<Object, Object> producer = new KafkaProducer<>(properties);
            WebSocketClient client = getWebSocketClient(FINNHUB_TOKEN, mapper, producer);
            client.connect();
    }

}
