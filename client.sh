MAVEN_OPTS="-Djava.library.path=/usr/local/lib" mvn exec:java -Dexec.mainClass=de.jexp.zmq.CypherClient -Dexec.arg="${1-start n=node(0) return n}"
