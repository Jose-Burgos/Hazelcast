package hazelcast.mapreduce;

import com.hazelcast.mapreduce.Collator;
import hazelcast.utils.Pair;

import java.util.*;

@SuppressWarnings("deprecation")
public class Query3Collator implements Collator<Map.Entry<Pair<String, Pair<Integer, Integer>>, Long>, List<Map.Entry<Pair<String, Pair<Integer, Integer>>, Long>>> {
    @Override
    public List<Map.Entry<Pair<String, Pair<Integer, Integer>>, Long>> collate(Iterable<Map.Entry<Pair<String, Pair<Integer, Integer>>, Long>> values) {
        List<Map.Entry<Pair<String, Pair<Integer, Integer>>, Long>> list = new ArrayList<>();
        for (var entry : values) {
            list.add(entry);
        }
        list.sort(Comparator
                .comparing((Map.Entry<Pair<String, Pair<Integer, Integer>>, Long> e) -> e.getKey().getFirst())
                .thenComparing(e -> e.getKey().getSecond().getFirst())
                .thenComparing(e -> e.getKey().getSecond().getSecond()));
        return list;
    }
}
