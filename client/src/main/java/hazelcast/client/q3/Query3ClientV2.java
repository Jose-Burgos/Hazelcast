package hazelcast.client.q3;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import hazelcast.client.Client;
import hazelcast.mapreduce.collator.Query3FinalOrderCollator;
import hazelcast.mapreduce.collator.Query3MaxDateCollator;
import hazelcast.mapreduce.combiner.Query3FirstCombiner;
import hazelcast.mapreduce.combiner.Query3SecondCombiner;
import hazelcast.mapreduce.map.Query3FirstMapper;
import hazelcast.mapreduce.map.Query3SecondMapper;
import hazelcast.mapreduce.reduce.Query3FirstReducer;
import hazelcast.mapreduce.reduce.Query3SecondReducer;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

public class Query3ClientV2 extends Client {
    private static final boolean USE_COMBINER = true;

    public static void main(String[] args) throws Exception {
        Query3Client client = new Query3Client();
        try {
            client.init();
            client.runQuery();
        } finally {
            client.shutdown();
        }
    }

    @Override
    public void runQuery() throws Exception {
        int w = Integer.parseInt(System.getProperty("w"));
        long startMapReduce = System.nanoTime();
        logger.info("Starting MapReduce job with window w = {}", w);

        JobTracker firstJobTracker = client.getJobTracker("query3-first-job-tracker");
        KeyValueSource<String, Complaint> kvSource = KeyValueSource.fromMap(complaintMap);
        Job<String, Complaint> firstJob = firstJobTracker.newJob(kvSource);

        Map<Pair<String, Pair<Integer, Integer>>, Long> firstResult;

        if (USE_COMBINER) {
            firstResult = firstJob.mapper(new Query3FirstMapper(validTypes, city.equals("NYC")))
                    .combiner(new Query3FirstCombiner())
                    .reducer(new Query3FirstReducer())
                    .submit()
                    .get();
        } else {
            firstResult = firstJob.mapper(new Query3FirstMapper(validTypes, city.equals("NYC")))
                    .reducer(new Query3FirstReducer())
                    .submit()
                    .get();
        }
        Map<String, Pair<Integer, Integer>> maxDates = new Query3MaxDateCollator().collate(firstResult.entrySet());

        IMap<Pair<String, Pair<Integer, Integer>>, Long> preAggregated = client
                .getMap("preAggregatedQuery3");
        preAggregated.putAll(firstResult);

        JobTracker secondJobTracker = client.getJobTracker("query3-second-job-tracker");
        KeyValueSource<Pair<String, Pair<Integer, Integer>>, Long> kvSource2 = KeyValueSource.fromMap(preAggregated);
        Job<Pair<String, Pair<Integer, Integer>>, Long> secondJob = secondJobTracker.newJob(kvSource2);
        List<String> finalResult;
        if (USE_COMBINER) {
            finalResult = secondJob.mapper(new Query3SecondMapper(maxDates, w))
                    .combiner(new Query3SecondCombiner())
                    .reducer(new Query3SecondReducer(w))
                    .submit(new Query3FinalOrderCollator())
                    .get();
        } else {
            finalResult = secondJob.mapper(new Query3SecondMapper(maxDates, w))
                    .reducer(new Query3SecondReducer(w))
                    .submit(new Query3FinalOrderCollator())
                    .get();

        }
        long endMapReduce = System.nanoTime();
        finalResult.add(0, "agency;year;month;moving_avg");
        Files.write(Paths.get(outPath, "query3_" + city + ".csv"), finalResult, StandardCharsets.UTF_8);

        List<String> timeLog = Arrays.asList(
                formatTimestamp() + " INFO [main] Started reading complaints",
                formatTimestamp() + " INFO [main] Finished reading complaints. Duration: "
                        + (endRead - startRead) / 1_000_000 + " ms",
                formatTimestamp() + " INFO [main] Started MapReduce job",
                formatTimestamp() + " INFO [main] Finished MapReduce job. Duration: "
                        + (endMapReduce - startMapReduce) / 1_000_000 + " ms");
        writeTimeLog("query3", timeLog);

        logger.info("Query3 completed successfully!");
    }
}
