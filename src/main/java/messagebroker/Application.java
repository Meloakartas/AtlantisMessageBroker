package messagebroker;

import com.rabbitmq.client.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@SpringBootApplication
public class Application {

    private static final String TASK_QUEUE_NAME = "metrics";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(1);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println(" [x] Received '" + message + "'");
            try {
                sendMetricToServer(message);
            } catch(Exception e) {
                System.out.println("Exception when executing work : " + e.getLocalizedMessage());
            } finally
            {
                System.out.println(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }

    private static void sendMetricToServer(String metric) throws IOException {
        URL url = new URL ("http://25.29.63.206:53144/ApiTest/api/RawMetric");
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("POST");
        con.setRequestProperty("User-Agent", "Java client");
        con.setRequestProperty("Content-Type", "application/json");

        try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
            byte[] input = metric.getBytes(StandardCharsets.UTF_8);
            wr.write(input, 0, input.length);
        }
        finally {
            con.disconnect();
        }
    }

}
