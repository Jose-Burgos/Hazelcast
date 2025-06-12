package hazelcast.mapreduce.collator;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.hazelcast.mapreduce.Collator;

public class Query4FinalCollator implements Collator<Map.Entry<String, Long>, List<String>> {
    private final int totalValidTypes;

    public Query4FinalCollator(int totalValidTypes) {
        this.totalValidTypes = totalValidTypes;
    }

    @Override
    public List<String> collate(Iterable<Map.Entry<String, Long>> entries) {
        return StreamSupport.stream(entries.spliterator(), false)
                .map(e -> {
                    double percentage = (e.getValue() * 100.0) / totalValidTypes;
                    double truncated = Math.floor(percentage * 100.0) / 100.0;
                    return new AbstractMap.SimpleEntry<>(e.getKey(), truncated);
                })
                .filter(e -> e.getValue() > 0)
                .sorted(Comparator.<Map.Entry<String, Double>>comparingDouble(Map.Entry::getValue).reversed()
                        .thenComparing(Map.Entry::getKey))
                .map(e -> e.getKey() + ";" + String.format("%.2f", e.getValue()))
                .collect(Collectors.toList());
    }
}
