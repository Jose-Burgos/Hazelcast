package hazelcast.mapreduce.combiner;


import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query4Combiner implements CombinerFactory<Pair<String, String>, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(Pair<String, String> key) {
        return new Combiner<>() {
            @Override
            public void combine(Long value) {
            }

            @Override
            public Long finalizeChunk() {
                return 1L;
            }
        };
    }
}
