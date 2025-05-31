package hazelcast.mapreduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import hazelcast.utils.Pair;

import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("deprecation")
public class Query4Reducer implements ReducerFactory<Pair<String, String>, Long, Set<String>> {
    @Override
    public Reducer<Long, Set<String>> newReducer(Pair<String, String> key) {
        return new Reducer<>() {
            private final Set<String> typeSet = new HashSet<>();

            @Override
            public void reduce(Long value) {
                typeSet.add(key.getSecond());
            }

            @Override
            public Set<String> finalizeReduce() {
                return typeSet;
            }
        };
    }
}
