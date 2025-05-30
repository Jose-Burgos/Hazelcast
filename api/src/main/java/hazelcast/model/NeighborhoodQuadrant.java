package hazelcast.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.IOException;

@Getter
@NoArgsConstructor
public class NeighborhoodQuadrant implements DataSerializable {
    private String neighborhood;
    private double quadrantLat;
    private double quadrantLong;

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(neighborhood);
        out.writeDouble(quadrantLat);
        out.writeDouble(quadrantLong);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        neighborhood = in.readUTF();
        quadrantLat = in.readDouble();
        quadrantLong = in.readDouble();
    }
}
