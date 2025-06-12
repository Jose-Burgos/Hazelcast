package hazelcast.client.q4;

import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazelcast.client.Client;
import hazelcast.mapreduce.Query4Collator;
import hazelcast.mapreduce.Query4Mapper;
import hazelcast.mapreduce.Query4Reducer;
import hazelcast.mapreduce.collator.Query4CollatorV2;
import hazelcast.mapreduce.combiner.Query4Combiner;
import hazelcast.mapreduce.reduce.Query4ReducerV2;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings("deprecation")
public class Query4Client extends Client {
    private static final boolean USE_COMBINER = true;

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

        List<Map.Entry<String, String>> finalResults;

        logger.info("Using Collator for ordering and percentage calculation...");

        if (USE_COMBINER) {
            finalResults = jobTracker.newJob(kvSource)
                    .mapper(new Query4Mapper(validTypes, city, neighbourhood))
                    .combiner(new Query4Combiner())
                    .reducer(new Query4ReducerV2())
                    .submit(new Query4CollatorV2())
                    .get();
        } else {
            finalResults = jobTracker.newJob(kvSource)
                    .mapper(new Query4Mapper(validTypes, city, neighbourhood))
                    .reducer(new Query4Reducer())
                    .submit(new Query4Collator())
                    .get();
        }
        long endMapReduce = System
                .nanoTime();

        logger.info("MapReduce job finished. Duration: {} ms", (endMapReduce - startMapReduce) / 1_000_000);

        List<String> outputLines = new ArrayList<>();
        outputLines.add("street;percentage");
        for (var entry : finalResults) {
            outputLines.add(entry.getKey() + ";" + entry.getValue());
        }

        Files.write(Paths.get(outPath, "query4_" + city + ".csv"), outputLines, StandardCharsets.UTF_8);

        List<String> timeLog = Arrays.asList(
                formatTimestamp() + " INFO [main] Started reading complaints",
                formatTimestamp() + " INFO [main] Finished reading complaints. Duration: "
                        + (endRead - startRead) / 1_000_000 + " ms",
                formatTimestamp() + " INFO [main] Started MapReduce job",
                formatTimestamp() + " INFO [main] Finished MapReduce job. Duration: "
                        + (endMapReduce - startMapReduce) / 1_000_000 + " ms");

        writeTimeLog("query4", timeLog);

        logger.info("Query4 completed successfully!");
    }
}
