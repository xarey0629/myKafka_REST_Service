FROM --platform=linux/amd64 openjdk
VOLUME /tmp
COPY target/*.jar app.jar
COPY configfile .
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app.jar"]