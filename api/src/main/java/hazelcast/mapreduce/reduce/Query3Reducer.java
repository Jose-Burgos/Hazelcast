package hazelcast.mapreduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query3Reducer implements ReducerFactory<Pair<String, Pair<Integer, Integer>>, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(Pair<String, Pair<Integer, Integer>> key) {
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
