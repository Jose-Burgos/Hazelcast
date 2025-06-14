package hazelcast.mapreduce.collator;
import com.hazelcast.mapreduce.Collator;
import hazelcast.utils.Pair;


import java.util.*;

@SuppressWarnings("deprecation")
public class Query1Collator implements Collator<Map.Entry<Pair<String, String>, Long>, List<Map.Entry<Pair<String, String>, Long>>> {
    @Override
    public List<Map.Entry<Pair<String, String>, Long>> collate(Iterable<Map.Entry<Pair<String, String>, Long>> values) {
        List<Map.Entry<Pair<String, String>, Long>> list = new ArrayList<>();
        for (var entry : values) {
            list.add(entry);
        }
        list.sort(Comparator
                .<Map.Entry<Pair<String, String>, Long>>comparingLong(e -> -e.getValue())
                .thenComparing(e -> e.getKey().getFirst())
                .thenComparing(e -> e.getKey().getSecond()));
        return list;
    }
}
