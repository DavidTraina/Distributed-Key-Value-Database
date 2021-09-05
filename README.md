# Distributed Key-Value Database

## Usage
### Quick Start
Prerequisites:
- Java 8+
- Apache Ant
- Setup passwordless ssh on localhost https://stackoverflow.com/a/16651742
#### Start Zookeeper
Extract `zookeeper-3.4.11.tar.gz`
```
tar -xf zookeeper-3.4.11.tar.gz
```

create a basic zookeeper config
```
cp zookeeper-3.4.11/conf/zoo_sample.cfg zookeeper-3.4.11/conf/zoo.cfg
```

start zookeeper on `localhost:2181`
```
./zookeeper-3.4.11/bin/zkServer.sh start
```

#### Build the project
```
ant
```

#### Start a cluster with 4 nodes
```
java -jar m2-ecs.jar sample_config 4 localhost 2181
```
```
ECS> start
```

#### Start the client
```
java -jar m2-client.jar
```
