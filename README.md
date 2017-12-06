# csv-db-maven-plugin

Usage:
mvn csv-db:list
mvn csv-db:import

Configuration:
<plugin>
  <groupId>eu.malanik</groupId>
  <artifactId>csv-db-maven-plugin</artifactId>
  <version>1.0-SNAPSHOT</version>
  <configuration>
    <csvDirectory>src/main/db/data</csvDirectory>
    <dbUrl>jdbc:postgresql://localhost:5432/postgres</dbUrl>
    <dbUser>user</dbUser>
    <dbPassword>password</dbPassword>
    <dbDriver>org.postgresql.Driver</dbDriver>
  </configuration>
</plugin>

