# Usage

#Before deploying the cluster, your environment should have the following development tools:

· go development environment

· Java compilation environment 

· mvn compilation tool

· gcc

· Junit (download Junit.4.12.jar and hamcrest-core-1.3.jar ,then put them under  .../jdk/lib)

**Note**:Junit is for regression test,Junit version can be chosen by yourself

### Build shared library

```bash
cd libsdk
sh build.sh
sudo cp libcfs.so /usr/lib/libcfs.so
```

### Build jar with dependencies

```bash
cd java
mvn clean package
```

### Deploy single-node cluster using docker

```bash
cd docker
./run_docker.sh -r
```

## Run the java test program

```
java -cp target/libchubaofs-1.0-SNAPSHOT-jar-with-dependencies.jar io.chubao.fs.TestCfsClient ls <dirpath>
java -cp target/libchubaofs-1.0-SNAPSHOT-jar-with-dependencies.jar io.chubao.fs.TestCfsClient read <filepath>
java -cp target/libchubaofs-1.0-SNAPSHOT-jar-with-dependencies.jar io.chubao.fs.TestCfsClient write <filepath>
```
