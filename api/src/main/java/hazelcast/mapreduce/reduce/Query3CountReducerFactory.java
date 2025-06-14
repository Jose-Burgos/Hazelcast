package hazelcast.mapreduce.reduce;

import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.Reducer;

import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("deprecation")
public class Query3CountReducerFactory
        implements ReducerFactory<String, Integer, Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String key) {
        return new Reducer<>() {
            private final AtomicInteger sum = new AtomicInteger(0);

            @Override
            public void reduce(Integer value) {
                sum.addAndGet(value);
            }

            @Override
            public Integer finalizeReduce() {
                return sum.get();
            }
        };
    }
}
