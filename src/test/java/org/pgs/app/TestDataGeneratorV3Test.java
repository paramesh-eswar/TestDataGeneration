package org.pgs.app;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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

    @Test
    public void validateSchemaMetaData_validMetadata_returnsEmpty() throws Exception {
        Path meta = Paths.get("src/main/resources/test_data.json");
        assertTrue(Files.exists(meta));

        JSONParser parser = new JSONParser();
        // load descriptor.json from classpath
        JSONObject descriptorJson = null;
        try (java.io.InputStreamReader rdr = new java.io.InputStreamReader(TestDataGeneratorV3.class.getClassLoader().getResourceAsStream("descriptor.json"))) {
            descriptorJson = (JSONObject) parser.parse(rdr);
        }

        // build metaData map the same way the generator does
        JSONArray arr = (JSONArray) parser.parse(new FileReader(meta.toFile()));
        Iterator<?> it = arr.iterator();
        Map<String, JSONObject> metaData = new LinkedHashMap<>();
        while (it.hasNext()) {
            JSONObject o = (JSONObject) it.next();
            if (!metaData.containsKey(o.get("name"))) {
                metaData.put(String.valueOf(o.get("name")), o);
            }
        }

        // invoke private static validateSchemaMetaData via reflection
        Method m = TestDataGeneratorV3.class.getDeclaredMethod("validateSchemaMetaData", JSONObject.class, Map.class, Long.class);
        m.setAccessible(true);
        Object res = m.invoke(null, descriptorJson, metaData, 3L);
        assertTrue(res instanceof String);
        assertEquals("", ((String) res).trim());
    }
}
