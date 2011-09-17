mvn test -Dtest=SerializerTest -Djub.customkey=%1 -Djub.consumers=CONSOLE,XML,H2 -Djub.db.file=data/benchmarks/database -Djub.xml.file=logs/benchmarks.xml -Djub.charts.dir=data/benchmarks/graphs
