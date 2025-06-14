package hazelcast.client.q3;

import com.hazelcast.core.IMap;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazelcast.client.Client;
import hazelcast.mapreduce.collator.MovingAvgCollator;
import hazelcast.mapreduce.map.MovingAvgMapper;
import hazelcast.mapreduce.map.Query3CountMapper;
import hazelcast.mapreduce.reduce.MovingAvgReducerFactory;
import hazelcast.mapreduce.reduce.Query3CountReducerFactory;
import hazelcast.model.Complaint;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SuppressWarnings("deprecation")
public class Query3Client extends Client {
    private static final String QUERY_NAME = "query3";
    private static final String INTERMEDIATE_MAP = "g12-query3-counts";

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

        List<String> logs = new ArrayList<>();
        logs.add(formatTimestamp() + " INFO  [Query3] Start JOB1: monthly count");

        JobTracker jt = client.getJobTracker("q3-tracker");
        KeyValueSource<String, Complaint> source1 = KeyValueSource.fromMap(complaintMap);

        ICompletableFuture<Map<String,Integer>> futureCounts =
                jt.newJob(source1)
                        .mapper(new Query3CountMapper(city))
                        .reducer(new Query3CountReducerFactory())
                        .submit();

        Map<String,Integer> counts = futureCounts.get();
        logs.add(formatTimestamp() + " INFO  [Query3] Finish JOB1");

        IMap<String,Integer> countMap = client.getMap(INTERMEDIATE_MAP);
        countMap.clear();
        countMap.putAll(counts);

        logs.add(formatTimestamp() + " INFO  [Query3] Start JOB2: moving average w=" + w);
        KeyValueSource<String,Integer> source2 = KeyValueSource.fromMap(countMap);

        ICompletableFuture<List<String>> futureAvgs =
                jt.newJob(source2)
                        .mapper(new MovingAvgMapper())
                        .reducer(new MovingAvgReducerFactory())
                        .submit(new MovingAvgCollator(w));

        List<String> movingAvgs = futureAvgs.get();
        logs.add(formatTimestamp() + " INFO  [Query3] Finish JOB2");

        String outCsv = Paths.get(outPath, QUERY_NAME + city + ".csv").toString();
        Files.write(Paths.get(outCsv), movingAvgs, StandardCharsets.UTF_8);

        writeTimeLog(QUERY_NAME, logs);
    }
}
