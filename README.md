## Michael's Tiny Java Experiments

### db2downloader

A crude JDBC client downloads the `OTDUIMP.UIMT_SGxx_*` segment tables from DB2 and streams each one to a
parquet file, with configurable row group chunkery.

Setup:
- create or link to a directory with the DB2 JDBC driver, at db2downloader/jdbc_drivers
- create a JSON file with the job details, example below
- Run! gradle db2downloader:run --args="/path/to/your.json"

example.json:
```json
{
  "url": "jdbc:db2://hostname:446/databasename:useJDBC4ColumnNameAndLabelSemantics=NO;",
  "username": "MICAHEL CORZIER",
  "password": "SUPERSECRET",
  "batchSize": 50000,
  "monthsBack": 60,
  "segments": ["SG01", "SG02"]
}
```

- `batchSize` is both the JDBC fetch size and the parquet row group size, in rows.
- `monthsBack` limits each table to claims dated on or after that many months before today. SG05
  (`UIMT_SG05_EXTENSION`) ignores it and is downloaded in full every run.
- `segments` picks which tables to download; leave it out (or empty) for all of them. Known segments:
  SG01, SG02, SG03, SG04, SG05, SG07, SG08, SG09, SG10, SG11, SG14, SG15, SG16, SG17, SG18, SG19.

Each segment is written to `output/<segment>-<today's date>.parquet`, relative to the working directory.

#### Native binaries for Linux (x86-64 and ARM64)

```
./build-native.sh amd64     # Intel/AMD EC2
./build-native.sh arm64     # AWS Graviton
./build-native.sh           # host architecture
```

Finished binaries land in `dist/db2downloader-linux-<arch>`. No JVM on the target host — they need
only glibc and libz, and start in ~35ms.

(They are copied out of `build/native/nativeCompile/` because `nativeCompile` clears that directory
on every run, so building one architecture would otherwise delete the other's binary.)

GraalVM cannot cross-compile: a binary's architecture is whatever the build host is. So the build
runs inside a container of the target platform. Building for the host architecture takes about a
minute; the other one goes through QEMU emulation and took ~13 minutes on an M-series Mac. If that
becomes annoying, build each architecture on its own machine (or a matrix CI job) rather than
emulating.

The binaries are compiled with `-march=compatibility`, the lowest CPU baseline on both
architectures, so they run on any Graviton or Intel/AMD generation. This job is bound by network
and disk, not vector width; use `-PnativeMarch=x86-64-v3` if that ever stops being true.

`NATIVE_TEST=1 ./build-native.sh <arch>` compiles the test suite into a native image and runs it
there, which is the only way to catch reflection problems before they hit a real job.

Getting this to work meant getting Hadoop off the runtime classpath, which it turns out was never
really needed:

- Parquet 1.18 can write through `LocalOutputFile` + `PlainParquetConfiguration`, and `parquet-hadoop`
  declares Hadoop as *provided*. Hadoop was on the classpath only because `build.gradle` asked for it.
- Two things still reached for it. Parquet's `CodecFactory` cannot even load without hadoop-common
  (one of its private methods returns a Hadoop type, and HotSpot verifies a class as a unit), so
  `PureJavaCodecFactory` replaces it — which also swaps JNI Snappy for pure-Java aircompressor, so
  there is no bundled `.so` to unpack. And `AvroWriteSupport` resolves its data model via Hadoop's
  `ReflectionUtils` unless you pass `withDataModel(...)` explicitly.
- `checkNoHadoopAtRuntime` fails the build if Hadoop ever comes back, since the failure would
  otherwise show up an hour later inside `nativeCompile`.

That took the runtime dependency tree from ~370 lines to ~50.

Reflection config for the closed-source DB2 JCC driver lives in
`db2downloader/src/main/resources/META-INF/native-image/` — see the README there. It is a seed; for
anything it misses, run the tracing agent against a real job:

```
./gradlew -Pagent db2downloader:run --args="/path/to/your.json"
./gradlew db2downloader:metadataCopy --task run --dir src/main/resources/META-INF/native-image
```

##### Verified so far

On both x86-64 and ARM64: config parsing, driver load, licensing, and T4 connection setup all work
in the native binary (confirmed by driving it at an unreachable host and getting a properly
formatted JCC `Connection refused` rather than a reflection error). The parquet write path is
covered by `nativeTest`.

What has *not* been exercised is a real DB2 result set end to end — the JDBC `ResultSet` -> Avro ->
parquet path may still hit reflection the agent needs to see, particularly around column types this
schema mapping does not cover. Run the agent once against a real job before trusting either binary.
