package com.mapr.demo.bluecoat;

import java.io.*;
import java.lang.Exception;
import java.lang.Thread;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import sun.nio.cs.US_ASCII;

import javax.xml.bind.DatatypeConverter;

public class Producer {

    private static KafkaProducer producer;
    private static final ArrayList<File> allFiles = new ArrayList<>();
    private static long messagesProcessed;
    private final String topic;
    private static String stats;
    private int current, last = 0;

    public Producer(final String topic, final File f) {
        this.topic = topic;
        this.stats = topic + "_stats";
        configureProducer();
        if (f.isDirectory()) {
            for (final File fileEntry : f.listFiles()) {
                if (fileEntry.isDirectory()) {
                    System.err.println("WARNING: skipping files in directory " + fileEntry.getName());
                } else {
                    allFiles.add(fileEntry);
                }
            }
        } else {
            allFiles.add(f);
        }
    }

    public void produce() throws IOException {
        long startTime = System.nanoTime();
        long last_update = 0;

        BufferedReader reader;
        Charset cs = new US_ASCII();

        for (final File f : allFiles) {

            System.out.println("Publishing data from " + f.getAbsolutePath());

            // Check if the file is gzipped or not
            if (f.getName().toUpperCase().endsWith(".GZ")) {
                InputStream fileStream = new FileInputStream(f);
                InputStream gzipStream = new GZIPInputStream(fileStream);
                Reader decoder = new InputStreamReader(gzipStream, cs);
                reader = new BufferedReader(decoder);
            } else {
                FileReader fr = new FileReader(f);
                reader = new BufferedReader(fr);
            }

            String line = reader.readLine();

            try {
                long lineNumber = 1;
                while (line != null) {
                    long current_time = System.nanoTime();

                    // Build a unique key for the line to be published
                    byte[] hash = MessageDigest.getInstance("MD5").digest(line.getBytes(Charsets.US_ASCII));
                    String key = DatatypeConverter.printHexBinary(hash) + lineNumber;

                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, line.getBytes(Charsets.US_ASCII));

                    producer.send(record, (RecordMetadata metadata, Exception e) -> messagesProcessed++);

                    // Print performance stats once per second
                    if ((Math.floor(current_time - startTime) / 1e9) > last_update) {
                        last_update++;
                        producer.flush();
                        printStatus(messagesProcessed, 1, startTime);
                    }

                    line = reader.readLine();
                    lineNumber++;
                }

            } catch (Exception e) {
                System.err.println("ERROR: " + e);
                System.err.println("Line :'" + line + "'");
                e.printStackTrace();
            }
        }

        producer.flush();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }

        System.out.println("Published " + messagesProcessed + " messages to stream.");
        System.out.println("Finished.");

        producer.close();
    }

    /**
     * Set the value for a configuration parameter. This configuration parameter specifies which class to use to
     * serialize the value of each message.
     */
    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(props);
    }

    public static void printStatus(long records_processed, int poolSize, long startTime) {
        long elapsedTime = System.nanoTime() - startTime;
        double rp = records_processed / ((double) elapsedTime / 1000000000.0) / 1000;
        System.out.printf("Throughput = %.2f Kmsgs/sec published. Threads = %d. Total published = %d.\n",
                rp,
                poolSize,
                records_processed);
        ProducerRecord<String, byte[]> stats_event = new ProducerRecord<>(stats, Long.toString(elapsedTime / 1000),
                new Double(rp).toString().getBytes(Charsets.US_ASCII));
        producer.send(stats_event);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Exception("Usage: java -cp bluecoat.jar com.mapr.demo.bluecoat.Producer stream:topic [file_name | directory]");
        }
        Producer p = new Producer(args[0], new File(args[1]));
        p.produce();
    }
}
