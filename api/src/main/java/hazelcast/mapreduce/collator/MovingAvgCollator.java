package hazelcast.mapreduce.collator;

import com.hazelcast.mapreduce.Collator;
import hazelcast.utils.Pair;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@SuppressWarnings("deprecation")
public class MovingAvgCollator
        implements Collator<Map.Entry<String, List<Pair<Integer,Integer>>>, List<String>> {

    private final int window;

    public MovingAvgCollator(int window) {
        this.window = window;
    }

    @Override
    public List<String> collate(Iterable<Map.Entry<String, List<Pair<Integer,Integer>>>> entries) {
        List<String> output = new ArrayList<>();

        for (Map.Entry<String, List<Pair<Integer,Integer>>> entry : entries) {
            String key = entry.getKey();
            List<Pair<Integer,Integer>> pairs = entry.getValue();

            Map<Integer,Integer> monthCount = new HashMap<>();
            for (Pair<Integer,Integer> p : pairs) {
                monthCount.put(p.getFirst(), p.getSecond());
            }
            int maxMonth = Collections.max(monthCount.keySet());
            for (int m = 1; m <= maxMonth; m++) {
                monthCount.putIfAbsent(m, 0);
            }

            List<Integer> months = new ArrayList<>(monthCount.keySet());
            Collections.sort(months);

            for (int i = 0; i < months.size(); i++) {
                int month = months.get(i);
                int start = Math.max(0, i - window + 1);
                double sum = 0;
                for (int j = start; j <= i; j++) {
                    sum += monthCount.get(months.get(j));
                }
                double avg = sum / (i - start + 1);

                BigDecimal bd = new BigDecimal(avg)
                        .setScale(2, RoundingMode.DOWN);

                if (bd.compareTo(BigDecimal.ZERO) == 0) {
                    continue;
                }

                output.add(String.format(
                        "%s;%02d;%s",
                        key, month, bd.toPlainString()
                ));
            }
        }

        output.sort(Comparator.comparing((String line) -> {
            String[] parts = line.split(";");
            return parts[0] + ";" + parts[1] + ";" + parts[2];
        }));

        return output;
    }
}
