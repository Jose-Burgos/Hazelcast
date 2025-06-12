package hazelcast.mapreduce;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import hazelcast.model.Complaint;
import hazelcast.utils.Pair;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

@SuppressWarnings("deprecation")
public class Query3Mapper implements Mapper<String, Complaint, Pair<String, Pair<Integer, Integer>>, Long> {
    private final Set<String> validTypes;
    private final String city;

    private static final DateTimeFormatter[] FORMATTERS = new DateTimeFormatter[] {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd")
    };

    public Query3Mapper(Set<String> validTypes, String city) {
        this.validTypes = validTypes;
        this.city = city.toUpperCase();
    }

    @Override
    public void map(String key, Complaint complaint, Context<Pair<String, Pair<Integer, Integer>>, Long> context) {
        if (!validTypes.contains(complaint.getType())) return;

        boolean isOpen = (city.equals("NYC") && !complaint.getStatus().equalsIgnoreCase("Closed")) ||
                (city.equals("CHI") && complaint.getStatus().equalsIgnoreCase("Open"));
        if (!isOpen) return;

        LocalDateTime dateTime = parseDate(complaint.getCreatedDate());

        context.emit(
                new Pair<>(complaint.getAgency(), new Pair<>(dateTime.getYear(), dateTime.getMonthValue())),
                1L
        );
    }

    private LocalDateTime parseDate(String dateStr) {
        for (DateTimeFormatter formatter : FORMATTERS) {
            try {
                if (formatter.toString().contains("HH")) {
                    return LocalDateTime.parse(dateStr, formatter);
                } else {
                    return LocalDate.parse(dateStr, formatter).atStartOfDay();
                }
            } catch (Exception ignored) {}
        }
        throw new RuntimeException("Unrecognized date format: " + dateStr);
    }
}
