package hazelcast.mapreduce.map;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query4SecondMapper implements Mapper<Pair<String, String>, Long, String, Long> {
    @Override
    public void map(Pair<String, String> key, Long value, Context<String, Long> context) {
        String street = key.getFirst();
        context.emit(street, 1L);
    }
}
