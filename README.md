csv-db-maven-plugin
===================

The csv-db-maven-plugin simplifies maintenance of configuration data in applications. 
It collects the data from csv files and imports this data to database tables. 
Whole system configuration could be summarized in one csv file 
or clustered by e.g. business components. 
CSV as source format enables sorting and comparing files and is easy to maintain in table editors.   

Goals
-----
| Name  | Description | Usage |
| ------------- | ------------- | ------------- |
| list  | List a short summary of data in csv files  | mvn csv-db:list |
| import  | Import csv data to target database  | mvn csv-db:import |

Configuration
-------------
```xml
<plugin>
  <groupId>eu.malanik</groupId>
  <artifactId>csv-db-maven-plugin</artifactId>
  <version>1.0</version>
  <dependencies>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <version>42.1.4</version>
    </dependency>
  </dependencies>
  <configuration>
    <csvDirectory>src/main/resources/db/data</csvDirectory>
    <dbUrl>jdbc:postgresql://localhost:5432/postgres</dbUrl>
    <dbUser>user</dbUser>
    <dbPassword>password</dbPassword>
    <dbDriver>org.postgresql.Driver</dbDriver>
  </configuration>
</plugin>
```

Configuration
-------------
| Parameter  | Required | Default  | Description | 
| ------------- | ------------- | ------------- | ------------- |
| csvDirectory  | yes  |  | Source directory with csv files  |
| dbUrl  | yes  | | Connection to target database   | 
| dbUser  | yes  |  | The user for database connection |
| dbPassword  | yes  |  | The password for database connection |
| dbDriver  | yes |  | JDBC driver class for database connection  |
| dbSchema  | no  |  | Database schema to use |
| dateFormat  | no  | dd.MM.yyyy  | Format for date values  |
| timestampFormat  | no | dd.MM.yyyy HH:mm:ss  | Format for timestamp values  |


Tested databases
----------------
- PostgreSQL
- H2



Example csv
----------------
CSV file name represents table name in database. The '__' (double underscore) in file name 
can be used as delimiter between table name and comment, like configuration__DEV.csv

First row in csv represents column names in table. For actual date or timestamp use 'now'.

![Alt text](/doc/csv.png?raw=true "csv example")

Data types are determined from database columns.
