package messagebroker;

import com.rabbitmq.client.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.*;
import java.net.HttpURLConnection;
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
                sendM(message);
            } catch(Exception e) {
                System.out.println("Exception when executing work : " + e.getMessage());
            } finally
            {
                System.out.println(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }

    private static void sendMetric(String metric) throws IOException {
        String url = "http://25.29.63.206:53144/ApiTest/api/RawMetric/";
        byte[] postData = metric.getBytes(StandardCharsets.UTF_8);
        HttpURLConnection con = null;

        try {

            URL myurl = new URL(url);
            con = (HttpURLConnection) myurl.openConnection();

            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("User-Agent", "Java client");
            con.setRequestProperty("Content-Type", "application/json");

            try (DataOutputStream wr = new DataOutputStream(con.getOutputStream())) {
                wr.write(postData);
            }

            StringBuilder content;

            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()))) {

                String line;
                content = new StringBuilder();

                while ((line = in.readLine()) != null) {
                    content.append(line);
                    content.append(System.lineSeparator());
                }
            }

            System.out.println(content.toString());

        } finally {

            con.disconnect();
        }
    }

//    private static void sendMetricToServer(String metric) throws IOException {
//        URL url = new URL ("http://25.29.63.206:53144/ApiTest/api/RawMetric/");
//        HttpURLConnection con = (HttpURLConnection)url.openConnection();
//        con.setDoOutput(true);
//        con.setRequestMethod("POST");
//        con.setRequestProperty("Content-Type", "application/json");
//
//        try (OutputStream wr = new OutputStream(con.getOutputStream())) {
//            byte[] input = metric.getBytes(StandardCharsets.UTF_8);
//            wr.write(input, 0, input.length);
//        }
//        catch(Exception e)
//        {
//            System.out.println("Exception when executing work : " + e.getMessage());
//        }
//        finally {
//            con.disconnect();
//        }
//    }

    private static void sendM(String metric) throws IOException {
        StringEntity entity = new StringEntity(metric,
                ContentType.APPLICATION_JSON);

        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost request = new HttpPost("http://25.29.63.206:53144/ApiTest/api/RawMetric/");
        request.setEntity(entity);

        HttpResponse response = httpClient.execute(request);
        System.out.println(response.getStatusLine().getStatusCode());
    }

}
