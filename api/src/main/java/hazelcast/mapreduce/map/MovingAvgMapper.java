package hazelcast.mapreduce.map;

import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Context;
import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class MovingAvgMapper
        implements Mapper<String, Integer, String, Pair<Integer,Integer>> {

    public MovingAvgMapper() {
    }

    @Override
    public void map(String key, Integer count, Context<String, Pair<Integer,Integer>> ctx) {
        String[] parts = key.split(";");
        String agency = parts[0];
        String year   = parts[1];
        int    month  = Integer.parseInt(parts[2]);
        ctx.emit(agency + ";" + year, new Pair<>(month, count));
    }
}
