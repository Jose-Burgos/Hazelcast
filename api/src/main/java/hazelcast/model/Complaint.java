package hazelcast.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.IOException;

@Getter
@NoArgsConstructor
public class Complaint implements DataSerializable {
    private String agency;
    private String type;
    private String status;
    private String createdDate; // TODO: Format: yyyy-MM-dd'T'HH:mm:ss
    private String borough;
    private double latitude;
    private double longitude;
    private String street;

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(agency);
        out.writeUTF(type);
        out.writeUTF(status);
        out.writeUTF(createdDate);
        out.writeUTF(borough);
        out.writeDouble(latitude);
        out.writeDouble(longitude);
        out.writeUTF(street);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        agency = in.readUTF();
        type = in.readUTF();
        status = in.readUTF();
        createdDate = in.readUTF();
        borough = in.readUTF();
        latitude = in.readDouble();
        longitude = in.readDouble();
        street = in.readUTF();
    }

    public static Complaint fromEntry(String line) {
        String[] parts = line.split(";", 0);
        Complaint complaint = new Complaint();

        if (parts.length == 9) {
            complaint.agency = parts[2];
            complaint.type = parts[3];
            complaint.status = parts[5];
            complaint.createdDate = parts[1];
            complaint.borough = parts[6];
            complaint.latitude = parseDoubleSafe(parts[7]);
            complaint.longitude = parseDoubleSafe(parts[8]);
            complaint.street = parts[4];

        } else if (parts.length == 13) {
            complaint.agency = parts[3];
            complaint.type = parts[2];
            complaint.status = parts[4];
            complaint.createdDate = parts[5];
            complaint.borough = parts[10];
            complaint.latitude = parseDoubleSafe(parts[11]);
            complaint.longitude = parseDoubleSafe(parts[12]);

            String streetDirection = parts[7];
            String streetName = parts[8];
            String streetType = parts[9];
            complaint.street = String.format("%s %s %s", streetDirection, streetName, streetType);

        } else {
            System.err.println("Unknown format: " + line);
            return null;
        }

        return complaint;
    }

    private static double parseDoubleSafe(String s) {
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

}
