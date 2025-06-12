package hazelcast.mapreduce.reduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import hazelcast.utils.Pair;

public class Query3SecondReducer implements ReducerFactory<Pair<String, Pair<Integer, Integer>>, Long, Double> {
    private final int w;

    public Query3SecondReducer(int w) {
        this.w = w;
    }

    @Override
    public Reducer<Long, Double> newReducer(Pair<String, Pair<Integer, Integer>> key) {
        return new Reducer<>() {
            private long sum = 0;

            @Override
            public void reduce(Long value) {
                sum += value;
            }

            @Override
            public Double finalizeReduce() {
                return sum > 0 ? sum / (double) w : null;
            }
        };
    }
}
