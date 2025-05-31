package hazelcast.client.q3;

import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import hazelcast.client.Client;
import hazelcast.mapreduce.Query3Mapper;
import hazelcast.mapreduce.Query3Reducer;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.math.RoundingMode;
import java.util.*;

@SuppressWarnings("deprecation")
public class Query3Client extends Client {
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

        JobTracker jobTracker = client.getJobTracker("query3-job-tracker");
        KeyValueSource<String, Complaint> kvSource = KeyValueSource.fromMap(complaintMap);

        Map<Pair<String, Pair<Integer, Integer>>, Long> result = jobTracker.newJob(kvSource)
                .mapper(new Query3Mapper(validTypes, city))
                .reducer(new Query3Reducer())
                .submit()
                .get();

        long endMapReduce = System.nanoTime();
        logger.info("MapReduce job finished. Duration: {} ms", (endMapReduce - startMapReduce) / 1_000_000);

        Map<String, Map<Integer, TreeMap<Integer, Long>>> grouped = new TreeMap<>();
        for (var entry : result.entrySet()) {
            String agency = entry.getKey().getFirst();
            int year = entry.getKey().getSecond().getFirst();
            int month = entry.getKey().getSecond().getSecond();
            long count = entry.getValue();

            grouped
                    .computeIfAbsent(agency, k -> new TreeMap<>())
                    .computeIfAbsent(year, k -> new TreeMap<>())
                    .put(month, count);
        }

        DecimalFormat df = new DecimalFormat("#.00");
        df.setRoundingMode(RoundingMode.HALF_UP);

        List<String> outputLines = new ArrayList<>();
        outputLines.add("agency;year;month;moving_avg");

        for (var agencyEntry : grouped.entrySet()) {
            String agency = agencyEntry.getKey();
            for (var yearEntry : agencyEntry.getValue().entrySet()) {
                int year = yearEntry.getKey();
                TreeMap<Integer, Long> months = yearEntry.getValue();

                TreeMap<Integer, Long> fullMonths = new TreeMap<>();
                for (int m = 1; m <= 12; m++) fullMonths.put(m, 0L);
                fullMonths.putAll(months);

                List<Integer> monthList = new ArrayList<>(fullMonths.keySet());
                for (int i = 0; i < monthList.size(); i++) {
                    int currentMonth = monthList.get(i);
                    long sum = 0;
                    int count = 0;

                    for (int j = Math.max(0, i - w + 1); j <= i; j++) {
                        sum += fullMonths.get(monthList.get(j));
                        count++;
                    }

                    double avg = (double) sum / count;
                    outputLines.add(agency + ";" + year + ";" + currentMonth + ";" + df.format(avg));
                }
            }
        }

        Files.write(Paths.get(outPath, "query3_" + city + ".csv"), outputLines, StandardCharsets.UTF_8);

        List<String> timeLog = Arrays.asList(
                formatTimestamp() + " INFO [main] Started reading complaints",
                formatTimestamp() + " INFO [main] Finished reading complaints. Duration: " + (endRead - startRead) / 1_000_000 + " ms",
                formatTimestamp() + " INFO [main] Started MapReduce job",
                formatTimestamp() + " INFO [main] Finished MapReduce job. Duration: " + (endMapReduce - startMapReduce) / 1_000_000 + " ms"
        );
        writeTimeLog("query3", timeLog);

        logger.info("Query3 completed successfully!");
    }
}
