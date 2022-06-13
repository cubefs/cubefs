# Usage

### Build jar with dependencies

```bash
cd java
./build.sh
```

### Deploy single-node cluster using docker

```bash
cd docker
./run_docker.sh -r
```

## Run the java test program

```
java -cp target/libcubefs-1.0-SNAPSHOT-jar-with-dependencies.jar io.chubao.fs.TestCfsClient ls <dirpath>
java -cp target/libcubefs-1.0-SNAPSHOT-jar-with-dependencies.jar io.chubao.fs.TestCfsClient read <filepath>
java -cp target/libcubefs-1.0-SNAPSHOT-jar-with-dependencies.jar io.chubao.fs.TestCfsClient write <filepath>
```
