package hazelcast.mapreduce.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query3Combiner implements CombinerFactory<Pair<String, Pair<Integer, Integer>>, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(Pair<String, Pair<Integer, Integer>> key) {
        return new Combiner<>() {
            private long count = 0;

            @Override
            public void combine(Long value) {
                count += value;
            }

            @Override
            public void reset() {
                count = 0;
            }

            @Override
            public Long finalizeChunk() {
                return count;
            }
        };
    }
}
