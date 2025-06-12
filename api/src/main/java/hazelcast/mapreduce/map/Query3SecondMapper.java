package hazelcast.mapreduce.map;

import java.util.Map;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import hazelcast.utils.Pair;

public class Query3SecondMapper
        implements Mapper<Pair<String, Pair<Integer, Integer>>, Long, Pair<String, Pair<Integer, Integer>>, Long> {
    private final Map<String, Pair<Integer, Integer>> maxDates;
    private final int w;

    public Query3SecondMapper(Map<String, Pair<Integer, Integer>> maxDates, int w) {
        this.maxDates = maxDates;
        this.w = w;
    }

    @Override
    public void map(Pair<String, Pair<Integer, Integer>> key, Long value,
            Context<Pair<String, Pair<Integer, Integer>>, Long> context) {
        String agency = key.getFirst();
        int year = key.getSecond().getFirst();
        int month = key.getSecond().getSecond();

        Pair<Integer, Integer> maxDate = maxDates.get(agency);
        int maxYear = maxDate.getFirst();
        int maxMonth = maxDate.getSecond();

        for (int i = 0; i < w; i++) {
            int targetMonth = month + i;

            if (targetMonth > 12)
                break;
            if (year > maxYear)
                break;
            if (year == maxYear && targetMonth > maxMonth)
                break;

            context.emit(new Pair<>(agency, new Pair<>(year, targetMonth)), value);
        }
    }
}
