# Remoting Experiments for Neo4j's Cypher

## Usage

* right now queries are hard coded in clients

    ./server.sh [db-dir]
    ./client.sh
    ruby client.rb

### Sample Session

    Request
    {"query","start n=({ids}) return n", "params": {"ids" : [1,2]},"stats": true,"result":true}

    Response:
    [n]
    [{"id":1,"data":{"name":"foo"}}]
    [{"id":2,"data":{"name":"bar"}}]
    {"time": 0, "rows": 2, "bytes": 100}

## Ideas:

Write a Cypher only endpoint for Neo4j that uses a fast transport and serialization across multiple client languages.
Cypher Results are streamed from the server. Transaction support. Multithreaded clients and servers
Client examples in ruby, python, php, c#, c, javascript, java, scala, clojure, erlang
Public installation on Heroku for testing

### Serialization
Nodes, Relationships and Paths are converted into maps and lists for serialization recursively
 
    Node : { id : id, [data : {foo:bar}]}
    Relationship : { id : id, start: id, end: id, type : "FOO",  [data : {foo:bar}]}
    Path {start: node, nodes: [nodes], relationships [relationships], end: node, lenght: 1}

Header with Columns, optional Footer with time, bytes, tx-id, error, exception, rows, update-counts for nodes, relationships, properties.

### Compactness

* leave off footer, enable when needed
* ignore results (fire & forget)

### Transactions

* provide a "tx" parameter with `begin`, `commit`,`rollback`
* `tx-id` will be reported in footer
* provie a `tx-id` parameter with the transaction id    
* transaction will be suspended, resumed per request (if a tx-id is provided) and finished and removed at rollback/commit

## Serialization

* fast, lightweight, portable

### MessagePack

* messagepack-lite (source, build, install in repo)

    hg clone https://bitbucket.org/sirbrialliance/msgpack-java-lite
    cd msgpack-java-lite
    ant
    mvn install:install-file -DgroupId=net.asdfa -DartifactId=msgpack -Dversion=0.0.1 -Dfile=dist/msgpack-java-lite.jar  -Dpackaging=jar -DgeneratePom=true

## Transport

* fast, lightweight, portable

### ZeroMQ

    brew install zeromq

#### Java

   git clone https://github.com/zeromq/jzmq
   cd jzmq

   ./autogen.sh
   ./configure
   make
   mvn clean install
   mvn install:install-file -DgroupId=org.zeromq -DartifactId=zmq -Dversion=2.1.0 -Dfile=src/zmq.jar  -Dpackaging=jar -DgeneratePom=true

#### Running as Neo4j Kernel Extension

   The cypher server is now packaged as a Neo4j Kernel Extension. So if you build the jar with mvn package and drop the jar in your neo4j environment
   either in /server/plugins or your classpath/build repository for embedded development, the extension will be started and listen on port 5555 by default.

#### Ruby

    # didn't work: 
    sudo gem install zmq -- --with-zmq-dir=/usr/local/ --with-zmq-lib=/usr/local/lib/

    # did work https://github.com/chuckremes/ffi-rzmq
    sudo gem install ffi ffi-rzmq zmqmachine

### Websockets

## Alternatives, Resources

* BSON: http://bsonspec.org/
* Protocol Buffers http://code.google.com/p/protobuf/

### MessagePack

* http://msgpack.org/
* used msgpack implementation: https://bitbucket.org/sirbrialliance/msgpack-java-lite/overview
* https://github.com/msgpack/msgpack-java/blob/master/src/test/java/org/msgpack/TestSimpleArrays.java
* http://blog.andrewvc.com/why-arent-you-using-messagepack discussion: http://news.ycombinator.com/item?id=2571729

### ZeroMQ

* http://www.zeromq.org/intro:get-the-software
* http://www.zeromq.org/bindings:java
* https://github.com/imatix/zguide/blob/master/examples/Java/hwserver.java
* https://github.com/imatix/zguide
* zeromq-c: http://api.zeromq.org/2-1:zmq-recv

* ruby installation problems https://gist.github.com/2791766
* https://github.com/andrewvc/learn-ruby-zeromq/blob/master/001_Socket_Types/003_req_rep.rb
* http://www.zeromq.org/blog:multithreading-magic
* http://zguide.zeromq.org/page:all#Multithreading-with-MQ
* http://sysgears.com/articles/load-balancing-work-between-java-threads-using-zeromq/
* http://zguide.zeromq.org/java:mtserver


### Alternatives:
* Spread http://www.spread.org/ Spread, a asynchronous messaging protocol
* Netty: https://netty.io/ high performance NIO server/client, only for Java/JVM
* Storm (uses ZeroMQ): http://storm-project.net/ Distributed and fault-tolerant realtime computation: stream processing, continuous computation, distributed RPC
* Thrift: http://thrift.apache.org/ Code generator and RPC middleware for cross-language client-server applications