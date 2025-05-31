package hazelcast.client.q4;

import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazelcast.client.Client;
import hazelcast.mapreduce.Query4Mapper;
import hazelcast.mapreduce.Query4Reducer;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;

@SuppressWarnings("deprecation")
public class Query4Client extends Client {
    public static void main(String[] args) throws Exception {
        Query4Client client = new Query4Client();
        try {
            client.init();
            client.runQuery();
        } finally {
            client.shutdown();
        }
    }

    @Override
    public void runQuery() throws Exception {
        String neighbourhood = System.getProperty("neighbourhood").toUpperCase().replace("_", " ");

        long startMapReduce = System.nanoTime();
        logger.info("Starting MapReduce job for neighbourhood: {}", neighbourhood);

        JobTracker jobTracker = client.getJobTracker("query4-job-tracker");
        KeyValueSource<String, Complaint> kvSource = KeyValueSource.fromMap(complaintMap);

        Map<Pair<String, String>, Set<String>> result = jobTracker.newJob(kvSource)
                .mapper(new Query4Mapper(validTypes, city, neighbourhood))
                .reducer(new Query4Reducer())
                .submit()
                .get();

        long endMapReduce = System.nanoTime();
        logger.info("MapReduce job finished. Duration: {} ms", (endMapReduce - startMapReduce) / 1_000_000);

        Map<String, Set<String>> streetTypeCounts = new HashMap<>();
        Set<String> allTypes = new HashSet<>();

        for (var entry : result.entrySet()) {
            String street = entry.getKey().getFirst();
            Set<String> types = entry.getValue();

            streetTypeCounts.computeIfAbsent(street, k -> new HashSet<>()).addAll(types);
            allTypes.addAll(types);
        }

        int totalTypes = allTypes.size();
        DecimalFormat df = new DecimalFormat("#.00");
        df.setRoundingMode(RoundingMode.DOWN);

        List<Map.Entry<String, String>> finalResults = streetTypeCounts.entrySet().stream()
                .map(e -> {
                    String street = e.getKey();
                    double percentage = (totalTypes == 0) ? 0 : (e.getValue().size() * 100.0 / totalTypes);
                    return Map.entry(street, df.format(percentage));
                })
                .sorted(Comparator
                        .<Map.Entry<String, String>>comparingDouble(e -> -Double.parseDouble(e.getValue()))
                        .thenComparing(Map.Entry::getKey))
                .toList();

        List<String> outputLines = new ArrayList<>();
        outputLines.add("street;percentage");
        for (Map.Entry<String, String> entry : finalResults) {
            outputLines.add(entry.getKey() + ";" + entry.getValue());
        }
        Files.write(Paths.get(outPath, "query4_" + city + ".csv"), outputLines, StandardCharsets.UTF_8);

        List<String> timeLog = Arrays.asList(
                formatTimestamp() + " INFO [main] Started reading complaints",
                formatTimestamp() + " INFO [main] Finished reading complaints. Duration: " + (endRead - startRead) / 1_000_000 + " ms",
                formatTimestamp() + " INFO [main] Started MapReduce job",
                formatTimestamp() + " INFO [main] Finished MapReduce job. Duration: " + (endMapReduce - startMapReduce) / 1_000_000 + " ms"
        );
        writeTimeLog("query4", timeLog);

        logger.info("Query4 completed successfully!");
    }
}
