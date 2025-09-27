package org.pgs.app;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

public class TestDataGeneratorV2Test {

    @Test
    public void loadDescriptor_and_generate_smoke() throws Exception {
        Path meta = Paths.get("src/main/resources/test_data.json");
        assertTrue(Files.exists(meta));

        Class<?> cls = TestDataGeneratorV2.class;
        Method m = cls.getDeclaredMethod("generateTestData", String.class, Long.class);
        m.setAccessible(true);

        Object out = m.invoke(null, meta.toString(), 5L);
        assertTrue(out instanceof Boolean);
        assertTrue((Boolean) out);

        // verify output file created next to metadata
        Path outFile = meta.getParent().resolve("test_data_output.csv");
        assertTrue(Files.exists(outFile));
        Files.deleteIfExists(outFile);
    }
}
