csv-db-maven-plugin
===================

The csv-db-maven-plugin simplifies maintenance of configuration data in applications. 
It collects the data from csv files and imports this data to database tables. 
Whole system configuration could be summarized in*one*csv file 
or clustered by e.g. environment (development, test, ..). 
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
  <configuration>
    <csvDirectory>src/main/resources/data</csvDirectory>
    <dbUrl>jdbc:postgresql://localhost:5432/postgres</dbUrl>
    <dbUser>user</dbUser>
    <dbPassword>password</dbPassword>
  </configuration>
</plugin>
```

Configuration
-------------
| Parameter  | Required | Default  | Description | 
| ------------- | ------------- | ------------- | ------------- |
| csvDirectory  | YES  |  | Source directory with csv files  |
| dbUrl  | YES  | | Connection to target database   | 
| dbUser  | YES  |  | The user for database connection |
| dbPassword  | YES  |  | The password for database connection |
| dbDriver  | NO  | org.postgresql.Driver  | Content Cell  |
| dbSchema  | NO  |  | Database schema to use |
| dateFormat  | NO  | dd.MM.yyyy  | Format for date values  |
| timestampFormat  | NO  | dd.MM.yyyy HH:mm:ss  | Format for timestamp values  |


Tested databases
----------------
- PostgreSQL
- H2





