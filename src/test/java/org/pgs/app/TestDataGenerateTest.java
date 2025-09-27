package org.pgs.app;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

public class TestDataGenerateTest {

    @Test
    public void validateSchemaMetaData_validMetadata_returnsTrue() throws Exception {
        // load sample metadata file (CSV) from resources
    Path sample = Paths.get("src/main/resources/test_data.csv");
        assertTrue(Files.exists(sample));

        // read file into string array using TestDataGenerate internal logic is not necessary here;
        // instead we exercise the private validateSchemaMetaData via reflection.
        Class<?> cls = TestDataGenerate.class;
        Method m = cls.getDeclaredMethod("validateSchemaMetaData", java.util.Map.class, boolean.class, Long.class);
        m.setAccessible(true);

        java.util.Map<String, String> map = new java.util.LinkedHashMap<>();
        // construct a minimal valid schema: one number column with range sufficient for rows
        map.put("account_id", "number|10-100");

        Object result = m.invoke(null, map, false, 5L);
        assertTrue(result instanceof Boolean);
        assertTrue((Boolean) result);
    }

    @Test
    public void generateTestData_createsOutputFile() throws Exception {
        // Use the small CSV metadata file in resources and run generator to create output
    Path meta = Paths.get("src/main/resources/test_data.csv");
        assertTrue(Files.exists(meta));

        // create a temp copy of the metadata file so output goes into a temp dir
    Path tmpDir = Files.createTempDirectory("tdg-test-");
    Path tmpMeta = tmpDir.resolve("meta.csv");
        Files.copy(meta, tmpMeta);

        // invoke generateTestData via reflection
        Class<?> cls = TestDataGenerate.class;
        Method m = cls.getDeclaredMethod("generateTestData", String.class, boolean.class, Long.class);
        m.setAccessible(true);

        Object out = m.invoke(null, tmpMeta.toString(), false, 3L);
        assertTrue(out instanceof Boolean);
        assertTrue((Boolean) out);

        // verify output file exists next to metadata
        Path outFile = tmpDir.resolve("meta_output.csv");
        assertTrue(Files.exists(outFile), "Expected output file created next to metadata");

        // cleanup
        Files.deleteIfExists(outFile);
        Files.deleteIfExists(tmpMeta);
        Files.deleteIfExists(tmpDir);
    }
}
