package hazelcast.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import hazelcast.model.Complaint;
import hazelcast.model.ComplaintType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public abstract class Client {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected HazelcastInstance client;
    protected IMap<String, ComplaintType> typeMap;
    protected IMap<String, Complaint> complaintMap;
    protected Set<String> validTypes;
    protected String addresses;
    protected String city;
    protected String inPath;
    protected String outPath;
    private static final int BATCH_SIZE      = 1000;
    private static final int NUM_WORKERS     = Runtime.getRuntime().availableProcessors();


    protected long startRead;
    protected long endRead;

    private static final String GROUP_NAME = "g12";
    private static final String GROUP_PASSWORD = "g12-pass";

    public void setUpClient() {
        addresses = System.getProperty("addresses");
        city = System.getProperty("city").toUpperCase();
        inPath = System.getProperty("inPath");
        outPath = System.getProperty("outPath");
        if (addresses == null || city == null || inPath == null || outPath == null) {
            throw new IllegalArgumentException("Missing parameters: -Daddresses, -Dcity, -DinPath, -DoutPath");
        }
        logger.info("Initializing client with parameters: addresses={}, city={}, inPath={}, outPath={}",
                addresses, city, inPath, outPath);

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig clientNetworkConfig = clientConfig.getNetworkConfig();
        GroupConfig groupConfig = new GroupConfig(GROUP_NAME, GROUP_PASSWORD);
        clientConfig.setGroupConfig(groupConfig);
        Arrays.stream(addresses.split(";")).forEach(clientNetworkConfig::addAddress);
        client = HazelcastClient.newHazelcastClient(clientConfig);

    }

    public void loadComplaintTypes() throws IOException {
        String typesFile = Paths.get(inPath, "serviceTypes" + city + ".csv").toString();

        typeMap = client.getMap("g12-complaintTypes");

        try (Stream<String> lines = Files.lines(Paths.get(typesFile), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(ComplaintType::fromEntry)
                    .forEach(ct -> typeMap.put(ct.getType(), ct));
        }

        validTypes = new HashSet<>(typeMap.keySet());
        logger.info("Loaded {} valid complaint types from {}", validTypes.size(), typesFile);

    }

    public void loadComplaints() throws InterruptedException {
        complaintMap = client.getMap("g12-complaints");
        AtomicInteger atomicId = new AtomicInteger(0);

        startRead = System.nanoTime();
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(BATCH_SIZE * 2);
        Path file = Paths.get(inPath, "serviceRequests" + city + ".csv");

        Thread producer = new Thread(() -> {
            try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                br.readLine();
                String line;
                while ((line = br.readLine()) != null) {
                    queue.put(line);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                for (int i = 0; i < NUM_WORKERS; i++) {
                    try { queue.put("__POISON_PILL__"); } catch (InterruptedException ignored) {}
                }
            }
        });
        producer.start();

        ExecutorService consumers = Executors.newFixedThreadPool(NUM_WORKERS);
        for (int i = 0; i < NUM_WORKERS; i++) {
            consumers.submit(() -> {
                Map<String, Complaint> batch = new HashMap<>(BATCH_SIZE);
                try {
                    while (true) {
                        String line = queue.take();
                        if ("__POISON_PILL__".equals(line)) break;
                        Complaint c = Complaint.fromEntry(line);
                        String id = String.valueOf(atomicId.getAndIncrement());
                        batch.put(id, c);
                        if (batch.size() >= BATCH_SIZE) {
                            complaintMap.putAll(batch);
                            batch.clear();
                        }
                    }
                    if (!batch.isEmpty()) {
                        complaintMap.putAll(batch);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        producer.join();
        consumers.shutdown();
        consumers.awaitTermination(1, TimeUnit.HOURS);
        endRead = System.nanoTime();
        long duration = endRead - startRead/1_000_000;
        logger.info("{} completed in {} ms", validTypes.size(), duration);
    }


    public void init() throws IOException {
        setUpClient();
        loadComplaintTypes();
        try {
            loadComplaints();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract void runQuery() throws Exception;

    public void shutdown() {
        if (client != null) {
            complaintMap.clear();
            typeMap.clear();
            HazelcastClient.shutdownAll();
        }
    }

    protected String formatTimestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss:SSSS"));
    }

    protected void writeTimeLog(String queryName, List<String> logs) throws IOException {
        String filename = Paths.get(outPath, "time_" + queryName + city + ".txt").toString();
        Files.write(Paths.get(filename), logs, StandardCharsets.UTF_8);
    }
}
