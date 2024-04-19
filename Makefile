build:
	mvn clean -DskipTests install
format:
	mvn com.coveo:fmt-maven-plugin:format

test:
	mvn test
