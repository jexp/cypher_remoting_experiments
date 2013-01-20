query="${1-start n=node(0) return n}"
echo $query
MAVEN_OPTS="-Djava.library.path=/usr/local/lib" mvn compile exec:java -Dexec.mainClass=de.jexp.zmq.CypherClient -Dexec.args="$query"
