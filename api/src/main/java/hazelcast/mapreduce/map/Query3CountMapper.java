package hazelcast.mapreduce.map;

import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Context;
import hazelcast.model.Complaint;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


@SuppressWarnings("deprecation")
public class Query3CountMapper implements Mapper<String, Complaint, String, Integer> {
    private final String city;

    private static final DateTimeFormatter DTF =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public Query3CountMapper(String city) {
        this.city = city.toUpperCase();
    }

    @Override
    public void map(String key, Complaint complaint, Context<String, Integer> ctx) {
        String status = complaint.getStatus();
        boolean isOpen = "NYC".equals(city)
                ? !"Closed".equalsIgnoreCase(status)
                : "Open".equalsIgnoreCase(status);
        if (!isOpen) {
            return;
        }

        String created = complaint.getCreatedDate();
        LocalDateTime dt = LocalDateTime.parse(created, DTF);

        String agency = complaint.getAgency();
        String keyOut = String.format(
                "%s;%d;%02d",
                agency,
                dt.getYear(),
                dt.getMonthValue()
        );

        ctx.emit(keyOut, 1);
    }
}