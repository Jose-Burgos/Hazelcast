package hazelcast.mapreduce.combiner;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.io.Serializable;

@SuppressWarnings("deprecation")
public class Query3CountCombinerFactory
        implements CombinerFactory<String, Integer, Integer>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public Combiner<Integer, Integer> newCombiner(String key) {
        return new Combiner<Integer, Integer>() {
            private int sum = 0;

            @Override
            public void combine(Integer value) {
                sum += value;
            }

            @Override
            public Integer finalizeChunk() {
                return sum;
            }

            @Override
            public void reset() {
                sum = 0;
            }
        };
    }
}