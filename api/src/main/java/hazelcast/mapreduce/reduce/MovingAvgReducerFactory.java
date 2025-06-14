package hazelcast.mapreduce.reduce;

import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.Reducer;
import hazelcast.utils.Pair;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("deprecation")
public class MovingAvgReducerFactory
        implements ReducerFactory<String, Pair<Integer,Integer>, List<Pair<Integer,Integer>>> {

    @Override
    public Reducer<Pair<Integer,Integer>, List<Pair<Integer,Integer>>> newReducer(String key) {
        return new Reducer<>() {
            private final List<Pair<Integer, Integer>> buffer = new ArrayList<>();

            @Override
            public void reduce(Pair<Integer, Integer> value) {
                buffer.add(value);
            }

            @Override
            public List<Pair<Integer, Integer>> finalizeReduce() {
                return buffer;
            }
        };
    }
}
