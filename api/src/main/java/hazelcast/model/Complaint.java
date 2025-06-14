package hazelcast.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
public class Complaint implements DataSerializable {
    private String id;
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
        out.writeUTF(id);
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
        id = in.readUTF();
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
            complaint.setId(parts[0]);
            complaint.setAgency(parts[2]);
            complaint.setType(parts[3]);
            complaint.setStatus(parts[5]);
            complaint.setCreatedDate(parts[1]);
            complaint.setBorough(parts[6]);
            complaint.setLatitude(parseDoubleSafe(parts[7]));
            complaint.setLongitude(parseDoubleSafe(parts[8]));
            complaint.setStreet(parts[4]);
        } else if (parts.length == 12) {
            complaint.setId(parts[0]);
            complaint.setAgency(parts[2]);
            complaint.setType(parts[1]);
            complaint.setStatus(parts[3]);
            complaint.setCreatedDate(parts[4]);
            complaint.setBorough(parts[9]);
            complaint.setLatitude(parseDoubleSafe(parts[10]));
            complaint.setLongitude(parseDoubleSafe(parts[11]));
            complaint.setStreet(String.format("%s %s %s %s", parts[5], parts[6], parts[7], parts[8]));

        } else

        {
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
