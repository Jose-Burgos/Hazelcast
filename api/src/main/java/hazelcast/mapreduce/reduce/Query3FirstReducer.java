package hazelcast.mapreduce.reduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import hazelcast.utils.Pair;

public class Query3FirstReducer implements ReducerFactory<Pair<String, Pair<Integer, Integer>>, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(Pair<String, Pair<Integer, Integer>> key) {
        return new Reducer<>() {
            private long sum = 0;

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
