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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
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

    public void loadComplaintTypes() throws IOException, CsvValidationException {
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

    public void loadComplaints() throws IOException, CsvValidationException {

        String complaintsFile = Paths.get(inPath, "serviceRequests" + city + ".csv").toString();
        logger.info("Starting to read complaints from: {}", complaintsFile);
        startRead = System.nanoTime();

        complaintMap = client.getMap("g12-complaints");
        AtomicInteger atomicId = new AtomicInteger(0);

        try (Stream<String> lines = Files.lines(Paths.get(complaintsFile), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(Complaint::fromEntry)
                    .forEach(complaint -> complaintMap.put(String.valueOf(atomicId.getAndIncrement()), complaint));
        }

        endRead = System.nanoTime();
        logger.info("Finished reading complaints. Duration: {} ms", (endRead - startRead) / 1_000_000);

    }

    public void init() throws IOException, CsvValidationException {
        setUpClient();
        loadComplaintTypes();
        loadComplaints();
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
