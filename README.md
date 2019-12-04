mvn archetype:generate \
	-DgroupId=com.goldcar.kafka \
	-DartifactId=java-api \
	-DarchetypeArtifactId=maven-archetype-quickstart \
	-DinteractiveMode=false


<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>2.3.1</version>
</dependency>

<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>1.7.29</version>
</dependency>

