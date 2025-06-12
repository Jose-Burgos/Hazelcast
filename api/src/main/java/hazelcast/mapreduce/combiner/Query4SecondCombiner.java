package hazelcast.mapreduce.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query4SecondCombiner implements CombinerFactory<String, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(String key) {
        return new Combiner<>() {
            private long sum = 0;

            @Override
            public void combine(Long value) {
                sum += value;
            }

            @Override
            public void reset() {
                sum = 0;
            }

            @Override
            public Long finalizeChunk() {
                return sum;
            }
        };
    }
}
