## Quick orientation

This repository is a small Java tool that generates CSV test data from a metadata descriptor
(JSON). It loads generation templates from `src/main/resources/descriptor.json` and
writes a CSV file next to the input metadata named `<input>_output.csv` (or `_output.csv`).

Key entry points (you may see several versions in the repo):
- `org.pgs.app.TestDataGenerate` — an older CLI entrypoint (simple CSV metadata format).
- `org.pgs.app.TestDataGeneratorV2` — a previous version (JSON-driven, single-threaded writer).
- `org.pgs.app.TestDataGeneratorV3` — a previous, feature-rich generator (multi-threaded writer).
- `org.pgs.app.TestDataGeneratorV4` - current, feature-rich generator (ExecutorService-based writer).
- `org.pgs.app.TestDataGeneratorUI` — Swing UI wrapper (also the assembly's mainClass).

In short: treat V3 as the authoritative implementation for adding features or fixing
generation logic; the UI and V2/Generate classes are kept for compatibility/examples.

## Important files and where to look
- `pom.xml` — Maven build. The maven-compiler-plugin's `<release>` is set in the POM
  (check `pom.xml` — currently it uses `release` 9 in this branch). The assembly plugin
  still produces a `jar-with-dependencies` whose manifest main is `org.pgs.app.TestDataGeneratorUI`.
- `src/main/resources/descriptor.json` — format templates (gender ranges, IP regex, uuid regex, etc.).
- `src/main/java/org/pgs/app/TestDataGeneratorV4.java` — V4 implementation: metadata parsing,
  per-datatype generation logic, multi-threaded (ExecutorService-based) CSV writing (inner class `WriteDataToFile`).
- `README.md` — sample metadata JSON schema (useful as authoritative example).

## Build and run (exact commands)
Prereqs: JDK that matches the POM `<release>` (check `pom.xml` — this branch currently sets `<release>` to 9).
Maven is required for building the fat JAR.

Build (from repo root):
```bash
mvn -DskipTests package
```

Artifact: the assembly plugin creates a jar-with-dependencies in `target/`.
Example run (fat JAR):
```bash
java -jar target/*-jar-with-dependencies.jar <metadata-file.json> <number-of-rows>
```
Notes on class-based runs (useful while developing):
- Run V3 directly from the classpath (prints help if you pass `--help`):
  java -cp target/*:target/classes org.pgs.app.TestDataGeneratorV4 <metadata-file.json> <numRows>
- Run the UI (Swing) from the IDE or via the assembled jar since the assembly manifest
  sets `org.pgs.app.TestDataGeneratorUI` as the main class.

Output: CSV written beside the metadata file (name depends on the tool variant: `_output.csv` or `_output.<ext>`).

## Metadata format & conventions (project-specific)
- V3 expects a JSON array of attribute descriptor objects (see `README.md` for examples).
- Common descriptor fields across the versions:
  - `name` — attribute name (required)
  - `datatype` — e.g. `number`, `text`, `float`, `date`, `gender`, `uuid`, `ipaddress`, `timestamp`, etc.
  - `range` — for numeric/date/timestamp/float types it's typically a `min~max` string; for `text` it may be an array of values.
  - `default_value` — a verbatim value to use instead of generated values.
  - `duplicates_allowed` — `yes`/`no` (controls uniqueness behavior for some types).
  - `date_format` / `timestamp_format` — DateTime patterns used by the generator when parsing/formatting.
  - `scale` — decimal scale for `float` types.
  - `cctype` — credit-card type for `creditcard` datatype (see `descriptor.json`).
  - `ipaddress_type` — `ipv4`, `ipv6` or `any`.

See `README.md` for concrete examples and copy/paste samples used by tests.

## Coding patterns to follow (and watch for)
- Generation logic uses `com.github.javafaker.Faker`, `json-simple` for JSON parsing and `opencsv` for CSV writes.
- Concurrency model in `TestDataGeneratorV3`:
  - A fixed number of `WriteDataToFile` worker threads (default 10).
  - Each worker is a `Thread` subclass with `isBusy`/`running` flags; the main controller sets work via `setValues(...)` and flips `isBusy`.
  - CSV writes are synchronized on the `CSVWriter` instance.
  - Important: V3 currently uses a busy-wait loop. If you change concurrency, prefer an ExecutorService or use wait/notify to avoid spin loops.

- Descriptor loading uses `TestDataGeneratorV3.class.getClassLoader().getResourceAsStream("descriptor.json")` so keep `descriptor.json` in `src/main/resources`.

## Dependencies and integration points
- The project declares these primary libs in `pom.xml`:
  - `com.opencsv:opencsv` (CSV writes)
  - `com.googlecode.json-simple:json-simple` (JSON parsing)
  - `com.github.javafaker:javafaker` (fake values)
  - `com.github.curious-odd-man:rgxgen` (regex generation for some descriptor formats)
- No network or external services; all generation is local and file-based.

## Common pitfalls and pointers
- Shared mutable maps: V3 passes and mutates shared maps such as `rangeSeq` and `floatSequence` across threads.
  The CSV write is synchronized, but the maps themselves are not — either use concurrent collections
  (ConcurrentHashMap) or synchronize access if you add parallel mutations.
- Logging: a lightweight logger `AppLogger` exists in `src/main/java/org/pgs/app/AppLogger.java`.
  Use `AppLogger.info|warn|error|debug(...)`. Enable debug with `-Dapp.debug=true` and redirect logs
  to a file with `-Dapp.logFile=/path/to/log`.
- Note: this branch shows several files that still print directly to stdout/stderr (e.g. `System.out.println` or `e.printStackTrace()` in `TestDataGenerate` and `TestDataGeneratorV2`). Prefer `AppLogger` for consistent, timestamped logs.
- String equality: avoid `==` or `!=` when comparing Strings; prefer `.isEmpty()` or `.trim().isEmpty()`.
  - Prefer the repository helper `Util.isBlank(Object)` for null-safe, trimmed emptiness checks. Example usage:
    - `if (Util.isBlank(obj.get("default_value"))) { /* treat as empty */ }`
    - `if (!Util.isBlank(entry.getValue().get("range"))) { /* range present */ }`
  - `Util.isBlank` is null-safe and calls `toString().trim().isEmpty()` under the hood, so prefer it when checking JSON values that may be null or non-String objects.
- Date parsing: `validateSchemaMetaData` validates date/timestamp formats; invalid patterns will be caught early.

## VS Code: launch configurations and tasks
If you use VS Code the workspace includes helpful debug/run wiring under `.vscode`:

- `.vscode/launch.json` contains class-based launch configs you can run from Run & Debug:
  - `TestDataGeneratorUI` — launches the Swing UI (pre-launch: `maven-package-skip-tests`).
  - `Debug TestDataGeneratorV4 (with args)` — launches V3 with example args and `-Dapp.debug` VM arg.
  - `Debug TestDataGeneratorUI (Swing)` — similar UI launcher with debug VM args.
  - `Run Assembled Jar (task)` — convenience entry that triggers the `run-assembled-jar` task which builds and runs the fat JAR.

- `.vscode/tasks.json` includes two tasks wired into the launch configs:
  - `maven-package-skip-tests` — runs `mvn -DskipTests package` (default preLaunch build task).
  - `run-assembled-jar` — runs `mvn -DskipTests package && java -jar target/*-jar-with-dependencies.jar <sample-args>`.

Notes:
- The `maven-package-skip-tests` task is used as a `preLaunchTask` so launching from VS Code builds the project first.
- The `run-assembled-jar` task runs the jar in a shell. VS Code does not automatically attach the debugger to that external `java -jar` process. To debug the jar, start it with remote debug flags and use an "Attach" debug configuration (see below).

Example: run the fat JAR with remote debugging enabled (attach later from VS Code):

```bash
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -jar target/*-jar-with-dependencies.jar <metadata.json> <numRows>
```

Then add/choose an "Attach" config in VS Code to connect to port 5005.

## Where to look when changing behavior
- To change CSV formatting or quoting: `TestDataGeneratorV4` — CSVWriter creation and synchronized write.
- To change how ranges are interpreted (e.g., inclusive/exclusive): `validateSchemaMetaData` and date/number
  handling in `TestDataGeneratorV4`.
- To change threading model: inner class `WriteDataToFile` in `TestDataGeneratorV4`.

If any part of this is unclear or you want more examples (unit tests, refactors, or a migration to
ExecutorService for threading), tell me which area to expand and I'll update this file.

## How to debug (quick checks and recommended breakpoints)

Typical debugging entry points:
- `org.pgs.app.TestDataGeneratorV4.main` — argument parsing, metadata path, timing.
- `org.pgs.app.TestDataGeneratorV4.generateTestData` — parsing + validation + dispatch loop.
- `org.pgs.app.TestDataGeneratorV4.WriteDataToFile.writeDataToFile` — per-row generation + synchronized CSV writes.

Recommended breakpoints:
- After metadata is parsed (inspect `metaData`).
- At `validateSchemaMetaData` to catch schema issues.
- Inside `WriteDataToFile.run` when `isBusy` is true (inspect row ranges and thread state).
- Immediately before/after `synchronized (writer) { writer.writeNext(...) }` to check wrote rows.

Quick runtime checks:
- Verify `descriptor.json` is visible at runtime: `TestDataGeneratorV4.class.getClassLoader().getResourceAsStream("descriptor.json")` should return non-null when launched from IDE or jar.
- Use `--help` on class launches to check usage messages.

Logging & instrumentation:
- Prefer `AppLogger` for consistent logs. Enable debug messages with `-Dapp.debug=true` and redirect to a file with `-Dapp.logFile=/path/to/log`.
- Default log file: if `-Dapp.logFile` is not provided, `AppLogger` will write to the default path `$HOME/.testdatagenerator/logs/app.log` (created automatically).
- Examples:
  - Explicit file: `java -Dapp.logFile=/absolute/path/to/testdata.log -jar target/*-jar-with-dependencies.jar <metadata.json> <numRows>`
  - Project-local file: `java -Dapp.logFile="$PWD/logs/testdata.log" -jar target/*-jar-with-dependencies.jar metadata.json 1000`
- If you still see `System.out.println` or `e.printStackTrace()` in some files (V2/V1), consider replacing them with `AppLogger` to keep behavior consistent across runs.
  
- Log rotation: AppLogger supports simple rotation controlled by system properties. Defaults are safe for local development.
  - `-Dapp.logRotatePolicy` — `daily` (default) or `size`.
  - `-Dapp.logMaxBytes` — maximum bytes before size-based rotation (default: 10485760 = 10MB).
  - `-Dapp.logBackupCount` — number of backups/rotations to keep (default: 5).
  - Behavior:
    - daily: previous day's file is moved to `app.log.YYYY-MM-DD` (numeric suffixes appended if needed up to backup count).
    - size: basic numeric rotation `app.log` -> `app.log.1`, `app.log.1` -> `app.log.2`, etc.
  - Examples:
    - Default daily rotation (no flags needed):
      `java -jar target/*-jar-with-dependencies.jar metadata.json 1000`
    - Force size rotation with 5MB limit and 3 backups:
      `java -Dapp.logRotatePolicy=size -Dapp.logMaxBytes=5000000 -Dapp.logBackupCount=3 -Dapp.logFile="$PWD/logs/testdata.log" -jar target/*-jar-with-dependencies.jar metadata.json 1000`

Concurrency tips:
- V3 uses busy-wait worker threads; if you see high CPU or stuck threads, reduce `numOfThreads` or refactor to an ExecutorService.
- Race conditions will often show up as duplicated/missing rows — focus on access to `numGenerators`, `rangeSequence`, and `floatSequence`.
