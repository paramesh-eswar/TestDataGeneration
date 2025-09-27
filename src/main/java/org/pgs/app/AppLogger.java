package org.pgs.app;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Small logging wrapper used across the project.
 * - Honors system property `-Dapp.debug=true` for debug-level messages.
 * - Lightweight and side-effect free (prints to stdout/stderr).
 */
public final class AppLogger {
    private static volatile boolean debug = Boolean.getBoolean("app.debug");
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    private static final Object LOCK = new Object();
    private static volatile PrintStream out = System.out;
    private static volatile PrintStream err = System.err;

    static {
        String logFile = System.getProperty("app.logFile");
        if (logFile != null && !logFile.trim().isEmpty()) {
            try {
                Path p = Paths.get(logFile);
                if (p.getParent() != null) Files.createDirectories(p.getParent());
                OutputStream fos = new FileOutputStream(p.toFile(), true);
                PrintStream ps = new PrintStream(fos, true, "UTF-8");
                out = ps;
                err = ps;
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try { ps.flush(); ps.close(); } catch (Exception ignore) {}
                }));
            } catch (Exception e) {
                // fallback to stdout/stderr
                out = System.out;
                err = System.err;
                err.println("AppLogger: failed to open log file " + logFile + ", using stdout");
            }
        }
    }

    private AppLogger() {}

    public static void setDebug(boolean on) { debug = on; }

    public static void info(String msg) {
        String s = format("INFO", msg);
        synchronized (LOCK) { out.println(s); }
    }

    public static void warn(String msg) {
        String s = format("WARN", msg);
        synchronized (LOCK) { out.println(s); }
    }

    public static void error(String msg) {
        String s = format("ERROR", msg);
        synchronized (LOCK) { err.println(s); }
    }

    public static void debug(String msg) {
        if (debug) {
            String s = format("DEBUG", msg);
            synchronized (LOCK) { out.println(s); }
        }
    }

    private static String format(String level, String msg) {
        return String.format("%s [%s] %s", TS.format(Instant.now()), level, msg);
    }
}
