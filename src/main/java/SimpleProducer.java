import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class SimpleProducer {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        String queueName = "test_queue";
        channel.queueDeclare(queueName, false, false, false, null);

        while (true) {
            channel.basicPublish("", queueName, null, Instant.now().toString().getBytes());
            Thread.sleep(5000);
        }
    }
}

class SimpleReceiver {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String queueName = "test_queue";
        channel.queueDeclare(queueName, false, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            var message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received: '" + message + "'");

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        System.out.println("Waiting for messages...");

        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {});
    }
}