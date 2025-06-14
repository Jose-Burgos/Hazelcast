package hazelcast.mapreduce.combiner;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query2CombinerFactory
        implements CombinerFactory<Pair<String, Pair<Integer, Integer>>, String, Map<String, Long>> {
    @Override
    public Combiner<String, Map<String, Long>> newCombiner(Pair<String, Pair<Integer, Integer>> key) {
        return new Combiner<>() {
            private final Map<String, Long> countMap = new HashMap<>();

            @Override
            public void combine(String value) {
                countMap.merge(value, 1L, Long::sum);
            }

            @Override
            public Map<String, Long> finalizeChunk() {
                return new HashMap<>(countMap);
            }

            @Override
            public void reset() {
                countMap.clear();
            }
        };
    }
}
