package org.pgs.app;

import static org.junit.jupiter.api.Assertions.*;

import java.io.FileReader;
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

/**
 * Lightweight, non-threading tests for TestDataGeneratorV3.
 * These avoid calling the multi-threaded generate path so they are safe for CI.
 */
public class TestDataGeneratorV3BasicTest {

    @Test
    public void loadDescriptor_notNull() throws Exception {
        JSONParser parser = new JSONParser();
        Method load = TestDataGeneratorV3.class.getDeclaredMethod("loadDescriptor", JSONParser.class);
        load.setAccessible(true);
        Object result = load.invoke(null, parser);
        assertNotNull(result, "descriptor.json should be loadable from classpath");
        assertTrue(result instanceof JSONObject);
    }

    @Test
    public void validateSchemaMetaData_validSample_returnsEmptyString() throws Exception {
        Path meta = Paths.get("src/main/resources/test_data.json");
        assertTrue(Files.exists(meta), "sample metadata must exist in resources");

        JSONParser parser = new JSONParser();
        // load descriptor via the class private method to ensure same behavior
        Method load = TestDataGeneratorV3.class.getDeclaredMethod("loadDescriptor", JSONParser.class);
        load.setAccessible(true);
        JSONObject descriptorJson = (JSONObject) load.invoke(null, parser);
        assertNotNull(descriptorJson);

        // build metaData map the same way generator does
        JSONArray arr = (JSONArray) parser.parse(new FileReader(meta.toFile()));
        Iterator<?> it = arr.iterator();
        Map<String, JSONObject> metaData = new LinkedHashMap<>();
        while (it.hasNext()) {
            JSONObject o = (JSONObject) it.next();
            if (!metaData.containsKey(o.get("name"))) {
                metaData.put(String.valueOf(o.get("name")), o);
            }
        }

        Method validate = TestDataGeneratorV3.class.getDeclaredMethod("validateSchemaMetaData", JSONObject.class, Map.class, Long.class);
        validate.setAccessible(true);
        String res = (String) validate.invoke(null, descriptorJson, metaData, 3L);
        assertNotNull(res);
        assertEquals("", res.trim(), "expected no validation errors for sample metadata");
    }
}
