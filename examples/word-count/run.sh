mvn package exec:java -Dexec.mainClass=leap.data.examples.beam.WordCount \
-Dexec.args="--runner=FlinkRunner  --filesToStage=target/word-count-bundled-0.1.1.jar \
--output=target/counts" -Pflink-runner