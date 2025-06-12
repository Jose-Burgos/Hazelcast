package hazelcast.mapreduce;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

import java.util.Set;

@SuppressWarnings("deprecation")
public class Query2Mapper implements Mapper<String, Complaint, Pair<String, Pair<Integer, Integer>>, String> {
    private final Set<String> validTypes;
    private final double q;

    public Query2Mapper(Set<String> validTypes, double q) {
        this.validTypes = validTypes;
        this.q = q;
    }

    @Override
    public void map(String key, Complaint complaint, Context<Pair<String, Pair<Integer, Integer>>, String> context) {
        if (validTypes.contains(complaint.getType())) {
            String barrio = complaint.getBorough();
            int latCell = (int) Math.floor(complaint.getLatitude() / q);
            int lonCell = (int) Math.floor(complaint.getLongitude() / q);
            context.emit(new Pair<>(barrio, new Pair<>(latCell, lonCell)), complaint.getType());
        }
    }
}
