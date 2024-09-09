package com.vgarg.javaexamples.kafka.avroexamples;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class AvroUnionExample {

    public static void main(String[] args) {
        // Create a SampleRecord object with a String value in the union field
        SampleRecord recordWithString = new SampleRecord();
        recordWithString.setData("This is a string");

        // Create a SampleRecord object with an array of integers in the union field
        SampleRecord recordWithArray = new SampleRecord();
        recordWithArray.setData(Arrays.asList(1, 2, 3, 4, 5));

        // Specify the file where the Avro data will be written
        File outputFile = new File("sample_record.avro");

        // Serialize the records to the file
        serialize(outputFile, recordWithString, recordWithArray);

        // Deserialize the records from the file and print them
        deserialize(outputFile);
    }

    // Serialize the SampleRecord objects to a file
    private static void serialize(File file, SampleRecord... records) {
        try {
            DatumWriter<SampleRecord> datumWriter = new SpecificDatumWriter<>(SampleRecord.class);
            DataFileWriter<SampleRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

            dataFileWriter.create(records[0].getSchema(), file);

            for (SampleRecord record : records) {
                dataFileWriter.append(record);
            }

            dataFileWriter.close();
            System.out.println("Serialization successful.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Deserialize the SampleRecord objects from a file
    private static void deserialize(File file) {
        try {
            DatumReader<SampleRecord> datumReader = new SpecificDatumReader<>(SampleRecord.class);
            DataFileReader<SampleRecord> dataFileReader = new DataFileReader<>(file, datumReader);

            SampleRecord record;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next();

                Object data = record.getData();
                if (data instanceof CharSequence) {
                    System.out.println("String value: " + data);
                } else if (data instanceof java.util.List) {
                    System.out.println("Array of Integers: " + data);
                }
            }

            dataFileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
