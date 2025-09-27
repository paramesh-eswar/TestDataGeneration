package org.pgs.app;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Simple smoke tests for AppLogger.
 *
 * The AppLogger reads the system property `app.logFile` during class initialization,
 * so tests must set the property before forcing class initialization via Class.forName(..., true, ...).
 */
public class AppLoggerTest {

    private static void setStaticField(Class<?> cls, String name, Object value) throws Exception {
        Field f = cls.getDeclaredField(name);
        f.setAccessible(true);
        f.set(null, value);
    }

    @Test
    public void sizeBasedRotationCreatesBackup(@TempDir Path tmp) throws Exception {
        Path log = tmp.resolve("app.log");

        // configure rotation via reflection
        Class<?> c = Class.forName("org.pgs.app.AppLogger");
        setStaticField(c, "logPath", log);
        setStaticField(c, "rotatePolicy", "size");
        setStaticField(c, "maxBytes", 200L);
        setStaticField(c, "maxBackups", 2);

        // set streams to our file
        PrintStream ps = new PrintStream(new FileOutputStream(log.toFile(), true), true, "UTF-8");
        setStaticField(c, "out", ps);
        setStaticField(c, "err", ps);

        // write several messages to exceed maxBytes and trigger rotation
        for (int i = 0; i < 200; i++) {
            AppLogger.info("rotation-test-line-" + i);
        }

        // allow IO
        Thread.sleep(200);

        Path rotated = tmp.resolve("app.log.1");
        assertTrue(Files.exists(rotated), "Expected rotated file app.log.1 to exist");

        // cleanup
        try { ps.close(); } catch (Exception ignore) {}
    }

    @Test
    public void dailyRotationMovesTodayFile(@TempDir Path tmp) throws Exception {
        Path log = tmp.resolve("app.log");

        // create initial log content
        Files.write(log, "oldlog\n".getBytes(StandardCharsets.UTF_8));

        Class<?> c = Class.forName("org.pgs.app.AppLogger");
        setStaticField(c, "logPath", log);
        setStaticField(c, "rotatePolicy", "daily");
        // set rotationDate to yesterday to force rotation
        setStaticField(c, "rotationDate", LocalDate.now().minusDays(1));
        setStaticField(c, "maxBackups", 3);

        // set streams
        PrintStream ps = new PrintStream(new FileOutputStream(log.toFile(), true), true, "UTF-8");
        setStaticField(c, "out", ps);
        setStaticField(c, "err", ps);

        // trigger a log write which should rotate existing file (yesterday)
        AppLogger.info("new-log-entry");

        Thread.sleep(200);

        String dateSuffix = LocalDate.now().minusDays(1).format(AppLoggerTestUtil.DATE_SUFFIX_FMT);
        Path rotated = tmp.resolve("app.log." + dateSuffix);
        assertTrue(Files.exists(rotated), "Expected rotated file with date suffix to exist");

        List<String> newLines = Files.readAllLines(log);
        assertTrue(newLines.stream().anyMatch(l -> l.contains("new-log-entry")), "New log should contain the new entry");

        try { ps.close(); } catch (Exception ignore) {}
    }

    @Test
    public void writesToProvidedLogFile(@TempDir Path tmp) throws Exception {
        Path log = tmp.resolve("app.log");
        Files.createFile(log);
        System.setProperty("app.logFile", log.toString());

        // Force AppLogger class initialization after setting the system property
        Class<?> c = Class.forName("org.pgs.app.AppLogger", true, Thread.currentThread().getContextClassLoader());
        // call info via reflection to avoid accidental early class loading
        c.getMethod("info", String.class).invoke(null, "app-logger-test-message");

        // small wait to allow IO to flush
        Thread.sleep(100);

        List<String> lines = Files.readAllLines(log);
        assertTrue(lines.stream().anyMatch(l -> l.contains("app-logger-test-message")), "Log file should contain the test message");
    }
}
