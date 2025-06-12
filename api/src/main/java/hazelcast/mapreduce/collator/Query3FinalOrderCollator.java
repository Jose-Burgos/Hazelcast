package hazelcast.mapreduce.collator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.hazelcast.mapreduce.Collator;

import hazelcast.utils.Pair;

public class Query3FinalOrderCollator
        implements Collator<Map.Entry<Pair<String, Pair<Integer, Integer>>, Double>, List<String>> {
    @Override
    public List<String> collate(Iterable<Map.Entry<Pair<String, Pair<Integer, Integer>>, Double>> entries) {
        return StreamSupport.stream(entries.spliterator(), false)
                .filter(e -> e.getValue() != null)
                .sorted(Comparator
                        .comparing((Map.Entry<Pair<String, Pair<Integer, Integer>>, Double> e) -> e.getKey().getFirst())
                        .thenComparing(e -> e.getKey().getSecond().getFirst())
                        .thenComparing(e -> e.getKey().getSecond().getSecond()))
                .map(e -> {
                    double truncated = Math.floor(e.getValue() * 100.0) / 100.0;
                    if (truncated % 1.0 == 0.0) {
                        return null;
                    }
                    return e.getKey().getFirst() + ";" +
                            e.getKey().getSecond().getFirst() + ";" +
                            e.getKey().getSecond().getSecond() + ";" +
                            String.format("%.2f", truncated);
                })
                .collect(Collectors.toList());
    }
}
