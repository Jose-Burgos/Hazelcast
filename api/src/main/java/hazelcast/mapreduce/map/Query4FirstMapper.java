package hazelcast.mapreduce.map;

import java.util.Set;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query4FirstMapper implements Mapper<String, Complaint, Pair<String, String>, Long> {
    private static final long ONE = 1L;
    private final String barrio;
    private final Set<String> validTypes;
    private final boolean isNYC;

    public Query4FirstMapper(String barrio, Set<String> validTypes, boolean isNYC) {
        this.barrio = barrio.toUpperCase();
        this.validTypes = validTypes;
        this.isNYC = isNYC;
    }

    @Override
    public void map(String key, Complaint complaint, Context<Pair<String, String>, Long> context) {
        if (!barrio.equalsIgnoreCase(complaint.getBorough())) {
            return;
        }

        if (!validTypes.contains(complaint.getType())) {
            return;
        }

        String calle = getStreet(complaint);
        context.emit(new Pair<>(calle, complaint.getType()), ONE);
    }

    private String getStreet(Complaint c) {
        String address = c.getStreet().toUpperCase();
        if (isNYC) {
            String[] tokens = address.split(" ", 2);
            if (tokens.length == 2 && tokens[0].matches("\\d+")) {
                return tokens[1];
            }
            return address;
        } else {
            String[] tokens = address.split(" ", 2);
            return tokens[1];
        }
    }

}
