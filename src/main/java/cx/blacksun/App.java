package cx.blacksun;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import cx.blacksun.dto.UserV1;
import cx.blacksun.dto.UserV2;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     *
     * @param args The arguments of the program.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        // Serialize the UserV1 user to disk
        UserV1 margherita = UserV1.newBuilder()
            .setName("Margherita Easom")
            .setFavoriteNumber(9)
            .setFavoriteColor("green")
            .build();

        File file = new File("usersv1.avro");

        DatumWriter<UserV1> userv1DatumWriter = new SpecificDatumWriter<UserV1>(UserV1.class);
        DataFileWriter<UserV1> userv1DataFileWriter = new DataFileWriter<UserV1>(userv1DatumWriter);
        userv1DataFileWriter.create(margherita.getSchema(), file);
        userv1DataFileWriter.append(margherita);
        userv1DataFileWriter.close();

        // And then read and deserialize it as a UserV2 object
        DatumReader<UserV2> userv2DatumReader = new SpecificDatumReader<UserV2>(UserV2.class);
        DataFileReader<UserV2> userv2DataFileReader = new DataFileReader<UserV2>(file, userv2DatumReader);
        UserV2 userv2 = null;
        userv2 = userv2DataFileReader.next(userv2);
        System.out.println(userv2);
        userv2DataFileReader.close();

        // Serialize the UserV2 user to disk
        UserV2 simon = UserV2.newBuilder()
            .setName("Simon Bruce")
            .setFavoriteNumber(256)
            .setFavoriteColor("blue")
            .setAge(42)
            .build();

        file = new File("usersv2.avro");

        DatumWriter<UserV2> userv2DatumWriter = new SpecificDatumWriter<UserV2>(UserV2.class);
        DataFileWriter<UserV2> userv2DataFileWriter = new DataFileWriter<UserV2>(userv2DatumWriter);
        userv2DataFileWriter.create(simon.getSchema(), file);
        userv2DataFileWriter.append(simon);
        userv2DataFileWriter.close();

        // And then read and deserialize it as a UserV1 object
        DatumReader<UserV1> userv1DatumReader = new SpecificDatumReader<UserV1>(UserV1.class);
        DataFileReader<UserV1> userv1DataFileReader = new DataFileReader<UserV1>(file, userv1DatumReader);
        UserV1 userv1 = null;
        userv1 = userv1DataFileReader.next(userv1);
        System.out.println(userv1);
        userv1DataFileReader.close();
    }
}
