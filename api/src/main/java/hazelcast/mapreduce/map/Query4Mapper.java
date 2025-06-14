package hazelcast.mapreduce.map;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

import java.util.Set;

@SuppressWarnings("deprecation")
public class Query4Mapper implements Mapper<String, Complaint, Pair<String, String>, Long> {
    private final Set<String> validTypes;
    private final String city;
    private final String neighbourhood;

    public Query4Mapper(Set<String> validTypes, String city, String neighbourhood) {
        this.validTypes = validTypes;
        this.city = city.toUpperCase();
        this.neighbourhood = neighbourhood.toUpperCase().replace("_", " ");
    }

    @Override
    public void map(String key, Complaint complaint, Context<Pair<String, String>, Long> context) {
        if (!validTypes.contains(complaint.getType()))
            return;
        if (!complaint.getBorough().equalsIgnoreCase(neighbourhood))
            return;

        String street = getStreet(complaint);
        if (street == null || street.isEmpty())
            return;

        context.emit(new Pair<>(street, complaint.getType()), 1L);
    }

    private String getStreet(Complaint c) {
        String address = c.getStreet().toUpperCase();
        if (city.equals("NYC")) {
            String[] tokens = address.split(" ", 2);
            if (tokens.length == 2 && tokens[0].matches("\\d+")) {
                return tokens[1];
            }
            return address;
        } else if (city.equals("CHI")) {
            String[] tokens = address.split(" ", 2);
            return tokens[1];
        }
        return null;
    }
}
