# Dependencies

- `sbt`
- `AdoptJDK 1.11.0`

# Bootstrapping Project

# Developing

## Unit Testing

`sbt test`

# Integration Test

Build the assembly and start the spark docker cluster:
```bash
sbt assembly
docker compose up -d
```

Go into the spark master container:
```bash
docker exec -it sparkcluster-spark-master-1 bash
```

Within the spark master container, change to testdata directory which is
mounted with `./src/test/resources/` in this project folder. 
Then run `spark-submit`.

```bash
/opt/spark/bin/spark-submit --class io.github.heikujiqu.sparkDetection.Main --master spark://sparkcluster-spark-master-1:7077 /app/sparkDetection-assembly-0.0.1.jar
```

On host computer, spark history server is also exposed at `localhost:18080`, so that you can see past jobs.
