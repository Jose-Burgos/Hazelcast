package hazelcast.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.IOException;
import java.util.Arrays;

@Getter
@Setter
@NoArgsConstructor
public class ComplaintType implements DataSerializable {
    private String type;

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(type);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = in.readUTF();
    }

    public static ComplaintType fromParts(String[] parts) {
        if (parts.length == 1 || parts.length == 2) {
            ComplaintType ct = new ComplaintType();
            ct.setType(parts[0]);
            return ct;
        }
        System.err.println("Unknown format in ComplaintType: " + Arrays.toString(parts));
        return null;
    }

    public static ComplaintType fromEntry(String line) {

        String[] parts = line.split(";", -1);

        ComplaintType complaintType = new ComplaintType();
        if (parts.length == 2 || parts.length == 1) {
            complaintType.type = parts[0].trim();
            return complaintType;
        }
        throw new IllegalArgumentException("Invalid complaint type entry: " + line);
    }

}
