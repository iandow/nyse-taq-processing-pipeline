USAGE:
------

maprcli stream create -path /user/mapr/taq -produceperm p -consumeperm p -topicperm p -ttl 900
maprcli stream topic create -path /user/mapr/taq -topic trades -partitions 3
maprcli stream info -path /user/mapr/taq -json
maprcli stream topic info -path /user/mapr/taq -topic trades -json

java -cp finserv-streaming-1.0.jar:`mapr classpath` MyProducer /mapr/demo.mapr.com/data/taqtrade20131218 /user/mapr/taq:trades

java -cp finserv-streaming-1.0.jar:dependency-jars/* Consumer /user/mapr/taq:trades
