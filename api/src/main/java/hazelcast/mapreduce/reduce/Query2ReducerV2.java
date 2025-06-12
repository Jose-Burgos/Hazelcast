package hazelcast.mapreduce.reduce;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query2ReducerV2
        implements ReducerFactory<Pair<String, Pair<Integer, Integer>>, Map<String, Long>, String> {
    @Override
    public Reducer<Map<String, Long>, String> newReducer(Pair<String, Pair<Integer, Integer>> key) {
        return new Reducer<>() {
            private final Map<String, Long> countMap = new HashMap<>();

            @Override
            public void reduce(Map<String, Long> partialMap) {
                for (Map.Entry<String, Long> entry : partialMap.entrySet()) {
                    countMap.merge(entry.getKey(), entry.getValue(), Long::sum);
                }
            }

            @Override
            public String finalizeReduce() {
                return countMap.entrySet().stream()
                        .max(Map.Entry.<String, Long>comparingByValue()
                                .thenComparing(Map.Entry::getKey))
                        .map(Map.Entry::getKey)
                        .orElse("");
            }
        };
    }
}
