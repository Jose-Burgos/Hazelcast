package hazelcast.mapreduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query1Reducer implements ReducerFactory<Pair<String, String>, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(Pair<String, String> key) {
        return new Reducer<>() {
            private Long sum = 0L;

            @Override
            public void reduce(Long value) {
                sum += value;
            }

            @Override
            public Long finalizeReduce() {
                return sum;
            }
        };
    }
}
