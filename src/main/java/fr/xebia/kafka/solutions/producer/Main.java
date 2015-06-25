package fr.xebia.kafka.solutions.producer;


import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        XNewKafkaProducer counter = new XNewKafkaProducer("first");

        String brokerList = "localhost:9092";
        int delay = 500;
        int count = 10;

        /* start a producer */
        counter.configure(brokerList, "async");
        counter.start();

        long startTime = System.currentTimeMillis();
        System.out.println("Starting...");

        /* produce the numbers */
        for (int i = 0; i < count; i++) {
            counter.produce(Integer.toString(i));
            Thread.sleep(delay);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Done in " + (endTime - startTime) + " ms.");

        /* close shop and leave */
        counter.close();
        System.exit(0);

    }
}
