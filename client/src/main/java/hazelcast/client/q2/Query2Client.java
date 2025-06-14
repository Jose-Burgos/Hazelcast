package hazelcast.client.q2;

import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazelcast.client.Client;
import hazelcast.mapreduce.collator.Query2Collator;
import hazelcast.mapreduce.map.Query2Mapper;
import hazelcast.mapreduce.reduce.Query2Reducer;
import hazelcast.mapreduce.combiner.Query2CombinerFactory;
import hazelcast.mapreduce.reduce.Query2ReducerV2;
import hazelcast.utils.Pair;
import hazelcast.model.Complaint;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings("deprecation")
public class Query2Client extends Client {
    private static final boolean USE_COMBINER = true;

    public static void main(String[] args) throws Exception {
        Query2Client client = new Query2Client();
        try {
            client.init();
            client.runQuery();
        } finally {
            client.shutdown();
        }
    }

    @Override
    public void runQuery() throws Exception {
        double q = Double.parseDouble(System.getProperty("q"));
        long startMapReduce = System.nanoTime();
        logger.info("Starting MapReduce job with q = {}", q);

        JobTracker jobTracker = client.getJobTracker("query2-job-tracker");
        KeyValueSource<String, Complaint> kvSource = KeyValueSource.fromMap(complaintMap);

        List<Map.Entry<Pair<String, Pair<Integer, Integer>>, String>> sorted;

        if (USE_COMBINER) {
            sorted = jobTracker.newJob(kvSource)
                    .mapper(new Query2Mapper(validTypes, q))
                    .combiner(new Query2CombinerFactory())
                    .reducer(new Query2ReducerV2())
                    .submit(new Query2Collator())
                    .get();
        } else {
            sorted = jobTracker.newJob(kvSource)
                    .mapper(new Query2Mapper(validTypes, q))
                    .reducer(new Query2Reducer())
                    .submit(new Query2Collator())
                    .get();
        }

        long endMapReduce = System.nanoTime();
        logger.info("MapReduce job finished. Duration: {} ms", (endMapReduce - startMapReduce) / 1_000_000);

        List<String> outputLines = new ArrayList<>();
        outputLines.add("barrio;lat_cell;lon_cell;type");
        for (Map.Entry<Pair<String, Pair<Integer, Integer>>, String> entry : sorted) {
            outputLines.add(entry.getKey().getFirst() + ";" +
                    entry.getKey().getSecond().getFirst() + ";" +
                    entry.getKey().getSecond().getSecond() + ";" +
                    entry.getValue());
        }
        Files.write(Paths.get(outPath, "query2_" + city + ".csv"), outputLines, StandardCharsets.UTF_8);

        List<String> timeLog = Arrays.asList(
                formatTimestamp() + " INFO [main] Started reading complaints",
                formatTimestamp() + " INFO [main] Finished reading complaints. Duration: "
                        + (endRead - startRead) / 1_000_000 + " ms",
                formatTimestamp() + " INFO [main] Started MapReduce job",
                formatTimestamp() + " INFO [main] Finished MapReduce job. Duration: "
                        + (endMapReduce - startMapReduce) / 1_000_000 + " ms");
        writeTimeLog("query2", timeLog);

        logger.info("Query2 completed successfully!");
        sorted.clear();
    }
}
