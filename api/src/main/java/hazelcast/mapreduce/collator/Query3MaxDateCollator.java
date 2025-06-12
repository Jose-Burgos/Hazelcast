package hazelcast.mapreduce.collator;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.mapreduce.Collator;

import hazelcast.utils.Pair;

public class Query3MaxDateCollator implements
        Collator<Map.Entry<Pair<String, Pair<Integer, Integer>>, Long>, Map<String, Pair<Integer, Integer>>> {
    @Override
    public Map<String, Pair<Integer, Integer>> collate(
            Iterable<Map.Entry<Pair<String, Pair<Integer, Integer>>, Long>> entries) {
        Map<String, Pair<Integer, Integer>> maxDates = new HashMap<>();
        for (Map.Entry<Pair<String, Pair<Integer, Integer>>, Long> entry : entries) {
            String agency = entry.getKey().getFirst();
            Pair<Integer, Integer> yearMonth = entry.getKey().getSecond();
            Pair<Integer, Integer> currentMax = maxDates.get(agency);
            if (currentMax == null || isLater(yearMonth, currentMax)) {
                maxDates.put(agency, yearMonth);
            }
        }
        return maxDates;
    }

    private boolean isLater(Pair<Integer, Integer> a, Pair<Integer, Integer> b) {
        return a.getFirst() > b.getFirst() ||
                (a.getFirst().equals(b.getFirst()) && a.getSecond() > b.getSecond());
    }
}
