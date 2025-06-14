package hazelcast.mapreduce.collator;

import com.hazelcast.mapreduce.Collator;
import hazelcast.utils.Pair;

import java.text.DecimalFormat;
import java.math.RoundingMode;
import java.util.*;

@SuppressWarnings("deprecation")
public class Query4Collator implements Collator<Map.Entry<Pair<String, String>, Set<String>>, List<Map.Entry<String, String>>> {
    @Override
    public List<Map.Entry<String, String>> collate(Iterable<Map.Entry<Pair<String, String>, Set<String>>> values) {
        Map<String, Set<String>> streetTypeCounts = new HashMap<>();
        Set<String> allTypes = new HashSet<>();

        for (var entry : values) {
            String street = entry.getKey().getFirst();
            String type = entry.getKey().getSecond();
            streetTypeCounts.computeIfAbsent(street, k -> new HashSet<>()).add(type);
            allTypes.add(type);
        }

        int totalTypes = allTypes.size();
        DecimalFormat df = new DecimalFormat("#.00");
        df.setRoundingMode(RoundingMode.DOWN);

        return streetTypeCounts.entrySet().stream()
                .map(e -> {
                    String street = e.getKey();
                    double percentage = (totalTypes == 0) ? 0 : (e.getValue().size() * 100.0 / totalTypes);
                    return Map.entry(street, df.format(percentage));
                })
                .sorted(Comparator
                        .<Map.Entry<String, String>>comparingDouble(e -> -Double.parseDouble(e.getValue()))
                        .thenComparing(Map.Entry::getKey))
                .toList();
    }
}
