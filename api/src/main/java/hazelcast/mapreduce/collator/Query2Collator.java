package hazelcast.mapreduce;

import com.hazelcast.mapreduce.Collator;
import hazelcast.utils.Pair;

import java.util.*;

@SuppressWarnings("deprecation")
public class Query2Collator implements Collator<Map.Entry<Pair<String, Pair<Integer, Integer>>, String>, List<Map.Entry<Pair<String, Pair<Integer, Integer>>, String>>> {
    @Override
    public List<Map.Entry<Pair<String, Pair<Integer, Integer>>, String>> collate(Iterable<Map.Entry<Pair<String, Pair<Integer, Integer>>, String>> values) {
        List<Map.Entry<Pair<String, Pair<Integer, Integer>>, String>> list = new ArrayList<>();
        for (var entry : values) {
            list.add(entry);
        }
        list.sort(Comparator
                .comparing((Map.Entry<Pair<String, Pair<Integer, Integer>>, String> e) -> e.getKey().getFirst())
                .thenComparing(e -> e.getKey().getSecond().getFirst())
                .thenComparing(e -> e.getKey().getSecond().getSecond()));
        return list;
    }
}
