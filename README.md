USAGE:
------

Create the stream:

    `maprcli stream create -path /user/mapr/taq -produceperm p -consumeperm p -topicperm p -ttl 900`

Create the stream topic:

    `maprcli stream topic create -path /user/mapr/taq -topic trades -partitions 3`

Optionally, print stream info:

    `maprcli stream info -path /user/mapr/taq -json`
    `maprcli stream topic info -path /user/mapr/taq -topic trades -json`

Copy the finserv-streaming-1.0.jar and dependency-jars/ folder to a cluster node.

Then run the Producer like this:

    `java -cp finserv-streaming-1.0.jar:`mapr classpath` Producer /mapr/demo.mapr.com/data/taqtrade20131218 /user/mapr/taq:trades`

And the consumer like this:
    
    `java -cp finserv-streaming-1.0.jar:dependency-jars/* Consumer /user/mapr/taq:trades`
