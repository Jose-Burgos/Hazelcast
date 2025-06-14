package hazelcast.mapreduce.reduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query4ReducerV2 implements ReducerFactory<Pair<String, String>, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(Pair<String, String> key) {
        return new Reducer<>() {
            @Override
            public void reduce(Long value) {
            }

            @Override
            public Long finalizeReduce() {
                return 1L;
            }
        };
    }
}
