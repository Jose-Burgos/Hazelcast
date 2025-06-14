package hazelcast.client.q1;

import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazelcast.client.Client;
import hazelcast.mapreduce.collator.Query1Collator;
import hazelcast.mapreduce.map.Query1Mapper;
import hazelcast.mapreduce.reduce.Query1Reducer;
import hazelcast.mapreduce.combiner.Query1CombinerFactory;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings("deprecation")
public class Query1Client extends Client {
    private static final boolean USER_COMBINER = true;

    public static void main(String[] args) throws Exception {
        Query1Client client = new Query1Client();
        try {
            client.init();
            client.runQuery();
        } finally {
            client.shutdown();

        }
    }

    @Override
    public void runQuery() throws Exception {
        long startMapReduce = System.nanoTime();
        logger.info("Starting MapReduce job...");

        JobTracker jobTracker = client.getJobTracker("query1-job-tracker");
        KeyValueSource<String, Complaint> kvSource = KeyValueSource.fromMap(complaintMap);

        List<Map.Entry<Pair<String, String>, Long>> sorted;
        if (USER_COMBINER) {
            sorted = jobTracker.newJob(kvSource)
                    .mapper(new Query1Mapper(validTypes))
                    .combiner(new Query1CombinerFactory())
                    .reducer(new Query1Reducer())
                    .submit(new Query1Collator())
                    .get();
        } else {
            sorted = jobTracker.newJob(kvSource)
                    .mapper(new Query1Mapper(validTypes))
                    .reducer(new Query1Reducer())
                    .submit(new Query1Collator())
                    .get();
        }

        long endMapReduce = System.nanoTime();
        logger.info("MapReduce job finished. Duration: {} ms", (endMapReduce - startMapReduce) / 1_000_000);

        List<String> outputLines = new ArrayList<>();
        outputLines.add("type;agency;requests");
        for (Map.Entry<Pair<String, String>, Long> entry : sorted) {
            outputLines.add(entry.getKey().getFirst() + ";" +
                    entry.getKey().getSecond() + ";" +
                    entry.getValue());
        }
        Files.write(Paths.get(outPath, "query1_" + city + ".csv"), outputLines, StandardCharsets.UTF_8);
        List<String> timeLog = Arrays.asList(
                formatTimestamp() + " INFO [main] Started reading complaints",
                formatTimestamp() + " INFO [main] Finished reading complaints. Duration: "
                        + (endRead - startRead) / 1_000_000 + " ms",
                formatTimestamp() + " INFO [main] Started MapReduce job",
                formatTimestamp() + " INFO [main] Finished MapReduce job. Duration: "
                        + (endMapReduce - startMapReduce) / 1_000_000 + " ms");
        writeTimeLog("query1", timeLog);

        logger.info("Query1 completed successfully!");
        sorted.clear();
    }
}
