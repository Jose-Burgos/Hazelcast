package hazelcast.mapreduce;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

import java.util.Set;

@SuppressWarnings("deprecation")
public class Query1Mapper implements Mapper<String, Complaint, Pair<String, String>, Long> {
    private static final long ONE = 1L;
    private final Set<String> validTypes;

    public Query1Mapper(Set<String> validTypes) {
        this.validTypes = validTypes;
    }

    @Override
    public void map(String key, Complaint complaint, Context<Pair<String, String>, Long> context) {
        if (validTypes.contains(complaint.getType())) {
            context.emit(new Pair<>(complaint.getType(), complaint.getAgency()), ONE);
        }
    }
}
