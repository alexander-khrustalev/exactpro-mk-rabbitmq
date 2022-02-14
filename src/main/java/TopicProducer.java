import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class TopicProducer {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        channel.exchangeDeclare("t", BuiltinExchangeType.TOPIC, false, false, null);

        while (true) {
            channel.basicPublish("t", "hello.world", null, Instant.now().toString().getBytes());
            Thread.sleep(5000);
        }
    }
}

class TopicReceiver {
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, "t", "*.world");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            var message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received: '" + message + "'");

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        System.out.println("Waiting for messages...");

        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {});
    }
}