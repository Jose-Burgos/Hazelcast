package hazelcast.mapreduce.reduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import hazelcast.utils.Pair;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("deprecation")
public class Query2Reducer implements ReducerFactory<Pair<String, Pair<Integer, Integer>>, String, String> {
    @Override
    public Reducer<String, String> newReducer(Pair<String, Pair<Integer, Integer>> key) {
        return new Reducer<>() {
            private final Map<String, Long> countMap = new HashMap<>();

            @Override
            public void reduce(String value) {
                countMap.merge(value, 1L, Long::sum);
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
