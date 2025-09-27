package org.pgs.app;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
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
    private static volatile Path logPath = null;
    // rotation config
    private static volatile long maxBytes = 10 * 1024 * 1024; // 10 MB default
    private static volatile int maxBackups = 5;
    private static volatile String rotatePolicy = "daily"; // or "size"
    private static volatile LocalDate rotationDate = null;
    private static final DateTimeFormatter DATE_SUFFIX_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    static {
        String logFile = System.getProperty("app.logFile");
        // If caller didn't provide a log file, default to $HOME/.testdatagenerator/logs/app.log
        if (logFile == null || logFile.trim().isEmpty()) {
            String userHome = System.getProperty("user.home");
            try {
                logFile = Paths.get(userHome, ".testdatagenerator", "logs", "app.log").toString();
            } catch (Exception e) {
                // fallback to no file (stdout/stderr)
                logFile = null;
            }
        }

        if (logFile != null && !logFile.trim().isEmpty()) {
            try {
                Path p = Paths.get(logFile);
                if (p.getParent() != null) Files.createDirectories(p.getParent());
                OutputStream fos = new FileOutputStream(p.toFile(), true);
                PrintStream ps = new PrintStream(fos, true, "UTF-8");
                out = ps;
                err = ps;
                logPath = p;
                // rotation policy: "size" (default) or "daily"
                String policy = System.getProperty("app.logRotatePolicy");
                if (policy != null && !policy.trim().isEmpty()) rotatePolicy = policy.trim().toLowerCase();
                if ("daily".equals(rotatePolicy)) rotationDate = LocalDate.now();
                // read optional rotation properties
                try {
                    String maxBytesProp = System.getProperty("app.logMaxBytes");
                    if (maxBytesProp != null && !maxBytesProp.trim().isEmpty()) {
                        maxBytes = Long.parseLong(maxBytesProp);
                    }
                } catch (Exception ignore) {}
                try {
                    String backups = System.getProperty("app.logBackupCount");
                    if (backups != null && !backups.trim().isEmpty()) {
                        maxBackups = Integer.parseInt(backups);
                    }
                } catch (Exception ignore) {}
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

    /**
     * Rotate the log file if it exceeds configured max bytes.
     * This is a simple rotation that renames: app.log -> app.log.1, app.log.1 -> app.log.2, ...
     */
    private static void rotateIfNeeded() {
        if (logPath == null) return;
        try {
            if ("daily".equals(rotatePolicy)) { // default: daily rotation
                LocalDate now = LocalDate.now();
                if (rotationDate == null || !now.isAfter(rotationDate)) return;
                synchronized (LOCK) {
                    try { if (!now.isAfter(rotationDate)) return; } catch (Exception ex) {}
                    try { if (out != System.out) out.flush(); } catch (Exception ignore) {}
                    try { if (err != System.err) err.flush(); } catch (Exception ignore) {}

                    String base = logPath.getFileName().toString();
                    String dateSuffix = rotationDate.format(DATE_SUFFIX_FMT);
                    Path rotated = logPath.resolveSibling(base + "." + dateSuffix);
                    try {
                        if (Files.exists(rotated)) {
                            // try numeric suffixes
                            for (int i = 1; i <= maxBackups; i++) {
                                Path candidate = logPath.resolveSibling(base + "." + dateSuffix + "." + i);
                                if (!Files.exists(candidate)) {
                                    Files.move(logPath, candidate);
                                    break;
                                }
                            }
                        } else {
                            Files.move(logPath, rotated);
                        }
                    } catch (Exception ignore) {}

                    // reopen new log file and update rotationDate
                    try {
                        OutputStream fos = new FileOutputStream(logPath.toFile(), true);
                        PrintStream ps = new PrintStream(fos, true, "UTF-8");
                        out = ps;
                        err = ps;
                        rotationDate = LocalDate.now();
                    } catch (Exception e) {
                        out = System.out;
                        err = System.err;
                        System.err.println("AppLogger: failed to reopen log file after rotation, using stdout");
                    }
                }
                return;
            }

            // size-based rotation
            long size = 0L;
            try { size = Files.size(logPath); } catch (Exception e) { return; }
            if (size <= 0 || size < maxBytes) return;

            synchronized (LOCK) {
                // double-check inside lock
                try { if (Files.size(logPath) < maxBytes) return; } catch (Exception ex) { /* ignore */ }

                // close current streams if they are file-backed
                try { if (out != System.out) out.flush(); } catch (Exception ignore) {}
                try { if (err != System.err) err.flush(); } catch (Exception ignore) {}

                // perform rotation: highest -> drop, shift others up
                for (int i = maxBackups - 1; i >= 1; i--) {
                    Path src = logPath.resolveSibling(logPath.getFileName().toString() + "." + i);
                    Path dst = logPath.resolveSibling(logPath.getFileName().toString() + "." + (i + 1));
                    try {
                        if (Files.exists(src)) {
                            Files.deleteIfExists(dst);
                            Files.move(src, dst);
                        }
                    } catch (Exception ignore) {}
                }
                // move current log to .1
                try {
                    Path first = logPath.resolveSibling(logPath.getFileName().toString() + ".1");
                    Files.deleteIfExists(first);
                    Files.move(logPath, first);
                } catch (Exception ignore) {}

                // reopen new log file
                try {
                    OutputStream fos = new FileOutputStream(logPath.toFile(), true);
                    PrintStream ps = new PrintStream(fos, true, "UTF-8");
                    out = ps;
                    err = ps;
                } catch (Exception e) {
                    out = System.out;
                    err = System.err;
                    System.err.println("AppLogger: failed to reopen log file after rotation, using stdout");
                }
            }
        } catch (Throwable t) {
            // do not allow logging to throw
        }
    }

    private AppLogger() {}

    public static void setDebug(boolean on) { debug = on; }

    public static void info(String msg) {
        rotateIfNeeded();
        String s = format("INFO", msg);
        synchronized (LOCK) { out.println(s); }
    }

    public static void warn(String msg) {
        rotateIfNeeded();
        String s = format("WARN", msg);
        synchronized (LOCK) { out.println(s); }
    }

    public static void error(String msg) {
        rotateIfNeeded();
        String s = format("ERROR", msg);
        synchronized (LOCK) { err.println(s); }
    }

    public static void debug(String msg) {
        if (debug) {
            rotateIfNeeded();
            String s = format("DEBUG", msg);
            synchronized (LOCK) { out.println(s); }
        }
    }

    private static String format(String level, String msg) {
        return String.format("%s [%s] %s", TS.format(Instant.now()), level, msg);
    }
}
