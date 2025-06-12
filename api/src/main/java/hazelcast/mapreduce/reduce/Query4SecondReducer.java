package hazelcast.mapreduce.reduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query4SecondReducer implements ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(String key) {
        return new Reducer<>() {
            private long count = 0;

            @Override
            public void reduce(Long value) {
                count += value;
            }

            @Override
            public Long finalizeReduce() {
                return count;
            }
        };
    }
}
