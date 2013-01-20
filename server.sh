MAVEN_OPTS="-Djava.library.path=/usr/local/lib -Xmx256M -Xms256M -server -d64" mvn exec:java -Dexec.mainClass=de.jexp.zmq.CypherServer -Dexec.args=${1-graph.db}
