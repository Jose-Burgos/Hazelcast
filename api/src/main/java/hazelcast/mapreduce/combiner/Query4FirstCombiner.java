package hazelcast.mapreduce.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import hazelcast.utils.Pair;

public class Query4FirstCombiner implements CombinerFactory<Pair<String, String>, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(Pair<String, String> key) {
        return new Combiner<>() {
            @Override
            public void combine(Long value) {
                // No acumulamos porque la clave ya es única
            }

            @Override
            public Long finalizeChunk() {
                return 1L;
            }
        };
    }
}
