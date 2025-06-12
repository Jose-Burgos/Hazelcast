package hazelcast.mapreduce.reduce;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import hazelcast.utils.Pair;

public class Query4ReducerV2 implements ReducerFactory<Pair<String, String>, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(Pair<String, String> key) {
        return new Reducer<>() {
            @Override
            public void reduce(Long value) {
                // No acumulamos nada: la clave misma ya representa un (calle, tipo) único
            }

            @Override
            public Long finalizeReduce() {
                return 1L; // Siempre 1, ya que es una clave única después del shuffle
            }
        };
    }
}
