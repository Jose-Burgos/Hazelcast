package hazelcast.mapreduce.map;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

@SuppressWarnings("deprecation")
public class Query3FirstMapper implements Mapper<String, Complaint, Pair<String, Pair<Integer, Integer>>, Long> {
    private static final long ONE = 1L;
    private final boolean isNYC;
    private final Set<String> validTypes;

    public Query3FirstMapper(Set<String> validTypes, boolean isNYC) {
        this.isNYC = isNYC;
        this.validTypes = validTypes;
    }

    @Override
    public void map(String key, Complaint complaint, Context<Pair<String, Pair<Integer, Integer>>, Long> context) {
        // Filter by valid type first
        if (!validTypes.contains(complaint.getType())) {
            return;
        }

        boolean isOpen = isNYC
                ? !complaint.getStatus().equalsIgnoreCase("Closed")
                : complaint.getStatus().equalsIgnoreCase("Open");

        if (isOpen) {
            LocalDate date = LocalDate.parse(complaint.getCreatedDate().substring(0, 10));
            int year = date.getYear();
            int month = date.getMonthValue();
            context.emit(new Pair<>(complaint.getAgency(), new Pair<>(year, month)), ONE);
        }
    }
}
