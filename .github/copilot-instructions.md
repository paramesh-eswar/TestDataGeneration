## Quick orientation

This is a small Java CLI app that generates CSV test data from a metadata JSON descriptor.
It reads a metadata file (JSON array of attribute descriptors), consults a bundled
`descriptor.json` (in `src/main/resources`) for value-format templates, and writes a
CSV output file next to the input metadata file named `<input>_output.csv`.

Key classes:
- `org.pgs.app.TestDataGenerate` — original/older entry point.
- `org.pgs.app.TestDataGeneratorUI` — the main class referenced in the pom assembly manifest.
- `org.pgs.app.TestDataGeneratorV3` — newer generator implementation (multi-threaded writer).

Why this structure: the project evolved across versions (V2, V3). Each `TestDataGenerator*`
implements the metadata-driven generation logic; V3 contains the most feature-rich code
and is where you should start when changing generation rules or concurrency behavior.

## Important files and where to look
- `pom.xml` — Maven build. Java release target is 16; assembly plugin produces a
  `jar-with-dependencies` with main = `org.pgs.app.TestDataGeneratorUI`.
- `src/main/resources/descriptor.json` — format templates (gender ranges, IP regex, uuid regex, etc.).
- `src/main/java/org/pgs/app/TestDataGeneratorV3.java` — V3 implementation: metadata parsing,
  per-datatype generation logic, multi-threaded CSV writing (inner class `WriteDataToFile`).
- `README.md` — sample metadata JSON schema (useful as authoritative example).

## Build and run (exact commands)
Prereqs: JDK 16+ and Maven installed. The project uses the maven-assembly-plugin to make a fat jar.

Build (from repo root):
```bash
mvn -DskipTests package
```

Resulting artifact (examples already present in repo):
- `target/TestDataGeneration-*-jar-with-dependencies.jar` (or similar named jar in repo root)

Run (recommended fat-jar form):
```bash
java -jar target/*-jar-with-dependencies.jar <metadata-file.json> <number-of-rows>
```
Example:
```bash
java -jar target/TestDataGeneration-v3.0.1-SNAPSHOT-jar-with-dependencies.jar /path/metadata.json 10000
```

You can also print help directly:
```bash
java -cp target/*-jar-with-dependencies.jar org.pgs.app.TestDataGeneratorV3 --help
```

Output: a CSV file named `<metadata-filename>_output.csv` written to the same directory
as the metadata file.

## Metadata format & conventions (project-specific)
- The metadata is an array of JSON objects. Each object must include a `name` and `datatype`.
- Common keys you will see in code and descriptor lookups:
  - `range` — either a string `min~max` (numbers/dates/timestamps/floats) or a JSON array (text).
  - `default_value` — if non-empty it is used verbatim.
  - `duplicates_allowed` — `yes`/`no` — drives whether the generator must avoid duplicates.
  - `date_format` / `timestamp_format` — used for parsing or formatting dates.
  - `scale` — decimal places for `float` fields.
  - `cctype` — credit card type (see `descriptor.json` for allowed types).
  - `ipaddress_type` — `ipv4`, `ipv6` or `any`.

See `README.md` for a concrete, copy/paste sample metadata array used by the tests and examples.

## Coding patterns to follow (and watch for)
- Generation logic uses `com.github.javafaker.Faker` and `org.json.simple` for JSON parsing.
- Concurrency model in `TestDataGeneratorV3`:
  - A fixed number of `WriteDataToFile` threads (default 10).
  - Each thread is a `Thread` subclass with fields: `isBusy`, `running`, `started`.
  - Work is dispatched by setting `setValues(...)` and `isBusy = true`.
  - CSV write operations are synchronized on the `CSVWriter` instance.
  - Note: threads use a busy-wait loop (`while(running) { if(isBusy) { ... } }`). If you edit
    concurrency, consider replacing busy-wait with wait/notify or an ExecutorService.

- Descriptor loading uses `getResourceAsStream(DESCRIPTOR_FILE_PATH)` — keep `descriptor.json`
  on the classpath (src/main/resources). If tests or dev runs fail to find it, make sure resources
  are included on the classpath.

## Dependencies and integration points
- External libs visible in `pom.xml`:
  - `opencsv` for CSV writing
  - `json-simple` for JSON parsing
  - `javafaker` for fake data
  - `rgxgen` / regex-based generation used indirectly (descriptor regexes)
- No network/service integrations — all generation is local and file-based.

## Common pitfalls and pointers
- The V3 writer increments and mutates shared maps (e.g., `rangeSeq`, `floatSequence`). Those maps
  are passed around without fine-grained synchronization; the CSV write itself is synchronized but
  the maps are not. If you add parallel mutations, either make collections concurrent or synchronize.
- The code uses `String` equality checks like `toString() != ""` — prefer `.isEmpty()` or `.trim().isEmpty()`.
- Date parsing relies on `DateTimeFormatter` patterns in the metadata; invalid patterns are validated
  in `validateSchemaMetaData` and will fail fast with a readable message.

## Where to look when changing behavior
- To change CSV formatting or quoting: `TestDataGeneratorV3` — CSVWriter creation and synchronized write.
- To change how ranges are interpreted (e.g., inclusive/exclusive): `validateSchemaMetaData` and date/number
  handling in `TestDataGeneratorV3`.
- To change threading model: inner class `WriteDataToFile` in `TestDataGeneratorV3`.

If any part of this is unclear or you want more examples (unit tests, refactors, or a migration to
ExecutorService for threading), tell me which area to expand and I'll update this file.

## How to debug (quick checks and recommended breakpoints)

- Typical debugging entry points:
  - `org.pgs.app.TestDataGeneratorV3.main` — validate argument parsing, metadata file path, and timing.
  - `org.pgs.app.TestDataGeneratorV3.generateTestData` — metadata parsing, validation (`validateSchemaMetaData`), and the write loop that starts threads.
  - `org.pgs.app.TestDataGeneratorV3.WriteDataToFile.writeDataToFile` — per-row generation logic and the synchronized write to `CSVWriter`.

- Recommended breakpoints:
  - Just after metadata is parsed (after `parser.parse(new FileReader(...))`) to inspect `metaData` map.
  - At the start of `validateSchemaMetaData` to step through validation failures for bad metadata.
  - Inside `WriteDataToFile.run` when `isBusy` is true to inspect the `startRowNum` / `endRowNum` and thread state.
  - Immediately before and after `synchronized (writer) { writer.writeNext(...) }` to confirm written rows and to check for contention.

- Quick runtime checks (CLI / IDE):
  - Verify `descriptor.json` is on the classpath at runtime: inside your run configuration confirm resources folder is included, or programmatically test `TestDataGeneratorV3.class.getClassLoader().getResourceAsStream("descriptor.json")` returns non-null.
  - If the program exits with "Invalid arguments" messages, rerun with `--help` to see exact usage displayed by the app.
  - For file-not-found issues: print `new File(inputFilePath).getAbsolutePath()` and confirm file permissions.

- Logging and fast instrumentation:
  - The project uses `System.out.println` for status messages. Add temporary logging (SLF4J or java.util.logging) if you need log levels. For quick checks, add `System.out.printf("thread=%s start=%d end=%d\n", Thread.currentThread().getName(), startRowNum, endRowNum);` inside `WriteDataToFile`.
  - When diagnosing duplicates or sequence counts, dump shared maps (`rangeSequence`, `floatSequence`, `rangeSeq`) at key points. Because multiple threads mutate these maps, either snapshot them inside a synchronized block or use concurrent collections while debugging.

- Concurrency pitfalls to watch for while debugging:
  - Threads use busy-wait loops; if threads appear stuck, check `isBusy` flags and that the main loop is setting them. Also check the final waiter loop that checks `counter` for thread completion.
  - Race conditions often show as missing or duplicated rows — focus on how `numGenerators`, `rangeSeq` and `floatSequence` are mutated.

- Repro tips for debugging locally:
  - Reduce `numOfThreads` (in `generateTestData`) to 1 or 2 while stepping through thread logic to reduce noise.
  - Use small `numOfRows` (e.g., 1–100) for fast iteration.
  - Add a temporary `Thread.sleep(50)` inside `WriteDataToFile` after writing rows to make it easier to observe thread handoff in a debugger.

If you want, I can add a small logging wrapper and a `-Ddebug` flag to enable verbose output without editing source each time.
