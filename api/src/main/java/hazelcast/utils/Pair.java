package hazelcast.utils;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.Objects;

@Getter
@NoArgsConstructor
public class Pair<T extends Comparable<T>, K extends Comparable<K>> implements DataSerializable, Comparable<Pair<T, K>> {
    private T first;
    private K second;

    public Pair(T first, K second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(first);
        out.writeObject(second);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        first = in.readObject();
        second = in.readObject();
    }

    @Override
    public int compareTo(Pair<T, K> o) {
        int cmp = first.compareTo(o.first);
        return (cmp != 0) ? cmp : second.compareTo(o.second);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(first, pair.first) && Objects.equals(second, pair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}
