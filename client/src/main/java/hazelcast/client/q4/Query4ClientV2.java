package hazelcast.client.q4;

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
import hazelcast.mapreduce.collator.Query4FinalCollator;
import hazelcast.mapreduce.combiner.Query4FirstCombiner;
import hazelcast.mapreduce.combiner.Query4SecondCombiner;
import hazelcast.mapreduce.map.Query4FirstMapper;
import hazelcast.mapreduce.map.Query4SecondMapper;
import hazelcast.mapreduce.reduce.Query4FirstReducer;
import hazelcast.mapreduce.reduce.Query4SecondReducer;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

public class Query4ClientV2 extends Client {
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

        JobTracker firstJobTracker = client.getJobTracker("query4-first-job-tracker");
        KeyValueSource<String, Complaint> kvSource = KeyValueSource.fromMap(complaintMap);

        Job<String, Complaint> firstJob = firstJobTracker.newJob(kvSource);

        Map<Pair<String, String>, Long> firstResult;

        if (USE_COMBINER) {
            firstResult = firstJob
                    .mapper(new Query4FirstMapper(neighbourhood, validTypes, city.equals("NYC")))
                    .combiner(new Query4FirstCombiner())
                    .reducer(new Query4FirstReducer())
                    .submit()
                    .get();
        } else {
            firstResult = firstJob
                    .mapper(new Query4FirstMapper(neighbourhood, validTypes, city.equals("NYC")))
                    .reducer(new Query4FirstReducer())
                    .submit()
                    .get();

        }
        IMap<Pair<String, String>, Long> firstMap = client.getMap("g12-query4-first-map");
        firstMap.putAll(firstResult);

        JobTracker secondJobTracker = client.getJobTracker("query4-second-job-tracker");
        KeyValueSource<Pair<String, String>, Long> kvSource2 = KeyValueSource.fromMap(firstMap);

        Job<Pair<String, String>, Long> secondJob = secondJobTracker.newJob(kvSource2);
        List<String> finalResult;
        if (USE_COMBINER) {

            finalResult = secondJob
                    .mapper(new Query4SecondMapper())
                    .combiner(new Query4SecondCombiner())
                    .reducer(new Query4SecondReducer())
                    .submit(new Query4FinalCollator(validTypes.size()))
                    .get();
        } else {

            finalResult = secondJob
                    .mapper(new Query4SecondMapper())
                    .reducer(new Query4SecondReducer())
                    .submit(new Query4FinalCollator(validTypes.size()))
                    .get();

        }

        long endMapReduce = System.nanoTime();
        finalResult.add(0, "street;percentage");
        Files.write(Paths.get(outPath, "query4_" + city + ".csv"), finalResult, StandardCharsets.UTF_8);

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
