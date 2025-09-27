package org.pgs.app;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;

import org.junit.jupiter.api.Test;

public class TestDataGeneratorV3Test {

    @Test
    public void smokeTest_generateOneRow_createsOutputFile() throws Exception {
        // create temporary metadata file (one simple number column)
        File tmp = File.createTempFile("meta", ".json");
        tmp.deleteOnExit();
        String meta = "[ { \"name\": \"id\", \"datatype\": \"number\", \"range\": \"1~100\", \"default_value\": \"\", \"duplicates_allowed\": \"yes\" } ]";
        try (FileWriter fw = new FileWriter(tmp)) {
            fw.write(meta);
        }

        TestDataGeneratorV3 generator = new TestDataGeneratorV3();
        boolean ok = generator.generateTestData(tmp.getAbsolutePath(), 1L);
        assertTrue(ok, "generateTestData should return true for valid metadata");

        // output file should be next to metadata file and end with _output.csv
        String expected = tmp.getParentFile().getAbsolutePath() + File.separator + tmp.getName().replaceFirst("\\.json$", "") + "_output.csv";
        File out = new File(expected);
        assertTrue(out.exists(), "Output CSV should be created");
        out.delete();
    }

    @Test
    public void invalidMetadata_returnsFalse() throws Exception {
        File tmp = File.createTempFile("meta_invalid", ".json");
        tmp.deleteOnExit();
        String meta = "{ invalid json }";
        try (FileWriter fw = new FileWriter(tmp)) {
            fw.write(meta);
        }

        TestDataGeneratorV3 generator = new TestDataGeneratorV3();
        boolean ok = generator.generateTestData(tmp.getAbsolutePath(), 1L);
        assertFalse(ok, "generateTestData should return false for invalid metadata");
    }
}
