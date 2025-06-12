package hazelcast.mapreduce.combiner;

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import hazelcast.utils.Pair;

public class Query4Combiner implements CombinerFactory<Pair<String, String>, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(Pair<String, String> key) {
        return new Combiner<>() {
            @Override
            public void combine(Long value) {
                // No acumulamos, simplemente mantenemos la clave viva
            }

            @Override
            public Long finalizeChunk() {
                return 1L;
            }
        };
    }
}
