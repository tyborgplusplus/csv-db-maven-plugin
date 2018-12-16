csv-db-maven-plugin
===================

The csv-db-maven-plugin simplifies maintenance of read only configuration data in applications. 
It collects the data from csv files and imports this data to database tables. 
Whole system configuration could be summarized in csv files and every change could be tracked in source repository. 
CSV as source format enables sorting, merging and comparing files and is easy to maintain in table editors.   

Goals
-----
| Name  | Description | Usage |
| ------------- | ------------- | ------------- |
| list  | List a summary of data from csv files  | mvn csv-db:list |
| import  | Import csv data to target database  | mvn csv-db:import |

Usage
-------------
```xml
<plugin>
  <groupId>eu.malanik</groupId>
  <artifactId>csv-db-maven-plugin</artifactId>
  <version>2.0</version>
  <dependencies>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <version>42.2.5</version>
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
| csvDelimiter  | no  | , (comma) | Value delimiter in csv   | 
| dbUrl  | yes  | | Connection to target database   | 
| dbUser  | yes  |  | The user for database connection |
| dbPassword  | yes  |  | The password for database connection |
| dbDriver  | yes |  | JDBC driver class for database connection  |
| dbSchema  | no  |  | Database schema to use |
| dateFormat  | no  | dd.MM.yyyy  | Format for date values  |
| timestampFormat  | no | dd.MM.yyyy HH:mm:ss  | Format for timestamp values  |
| views  | no |  | List of views on csv files |


Tested databases
----------------
- PostgreSQL
- H2


Views
----------------
Since version 2.0 views could be configured in order to mix csv files for specific environment. 
Usually a generic configuration exists, which could be overridden by specific business or technical peculiarity.
Several views could be mixed together.     


CSV file names
----------------
CSV file name represents table name with several views. The '__' (double underscore) in file name 
will be used as delimiter between table name and views. Views will be delimited by '_' (simple underscore)


Example
----------------
##### Files: #####

- configuration.csv
- configuration__eu.csv
- configuration__eu_dev.csv
- configuration__eu_test.csv

##### Views: #####

```xml
<views>
  <view>eu</view>
  <view>test</view>
</views>
```


##### Result: #####

*configuration__eu_test.csv* overrides *configuration__eu.csv* which overrides *configuration.csv*

Overriding is based on primary keys, which will be determined from target database.
  
First row in csv represents column names in table. For actual date or timestamp use 'now'.


![Alt text](/doc/csv.png?raw=true "csv example")

Data types are determined from database columns.
