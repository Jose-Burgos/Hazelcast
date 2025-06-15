# Urban Complaints Processing with Hazelcast MapReduce

This project implements a distributed processing solution for urban complaints using **Hazelcast 3.8.6** and **Java**. The solution processes large volumes of complaint data from New York City (NYC) and Chicago (CHI), applying specific queries to generate useful statistics.

## Table of Contents

- [Requirements](#requirements)
- [Project Structure](#project-structure)
- [Compilation](#compilation)
- [Usage](#usage)
  - [Server](#server)
  - [Client](#client)
    - [Query 1: Complaint Counts by Type and Agency](#query-1-complaint-counts-by-type-and-agency)
    - [Query 2: Most Popular Type by Neighborhood and Grid](#query-2-most-popular-type-by-neighborhood-and-grid)
    - [Query 3: Moving Average of Open Complaints by Agency](#query-3-moving-average-of-open-complaints-by-agency)
    - [Query 4: Percentage of Complaint Types by Street in a Neighborhood](#query-4-percentage-of-complaint-types-by-street-in-a-neighborhood)
- [Author](#author)

## Requirements

- Java 11+
- Maven
- Hazelcast 3.8.6

## Project Structure

- `src/main/java/hazelcast/client`  
  Contains the base `Client` class and specific clients for each query.
- `src/main/java/hazelcast/mapreduce`  
  Contains the Mappers, Reducers, and Combiners for the queries.
- `src/main/java/hazelcast/model`  
  Model classes: `Complaint`, `ComplaintType`.
- `src/main/java/hazelcast/utils`  
  Utility classes like `Pair` and `CountPair`.


## Compilation

Compile the project using Maven:

```
mvn clean package
```

## Usage
### Server
Grab tpe2-g12-server-1.0-SNAPSHOT-bin.tar.gz 
```bash
tar xfz tpe2-g12-server-1.0-SNAPSHOT-bin.tar.gz 
cd tpe2-g12-server-1.0-SNAPSHOT
chmod +x run-sever.sh  
./run-server.sh [-Dinterface=<interface>] [-Dport=<port>]
```
Interface and port are optional parameters. If not specified, the default values are `localhost` and `5701`.

### Client
Grab tpe2-g12-client-1.0-SNAPSHOT-bin.tar.gz
```bash
tar xfz tpe2-g12-client-1.0-SNAPSHOT-bin.tar.gz 
cd tpe2-g12-client-1.0-SNAPSHOT
chmod +x query*  
```

The address format for the queries is:
```xxx.xxx.xxx.xxx:port```

#### Query 1: Complaint Counts by Type and Agency
```bash
sh query1.sh -Daddresses=<address-1;..;address-n> -Dcity=city  -DinPath=/path/to/input -DoutPath=/path/to/oputput
```
#### Query 2: Most Popular Type by Neighborhood and Grid
```bash
sh query2.sh -Daddresses=<address-1;..;address-n> -Dcity=city  -DinPath=/path/to/input -DoutPath=/path/to/oputput -Dq=0.1
```
#### Query 3: Moving Average of Open Complaints by Agency
```bash
sh query3.sh -Daddresses=<address-1;..;address-n> -Dcity=city -DinPath=/path/to/input -DoutPath=/path/to/oputput -Dw=window
```
#### Query 4: Percentage of Complaint Types by Street in a Neighborhood
```bash
sh query4.sh -Daddresses=<address-1;..;address-n> -Dcity=city  -DinPath=/path/to/input -DoutPath=/path/to/oputput -Dneighbourhood=BOROUGH_NAME
```

## Author
- Jose Burgos
- Pedro Curti
- Lautaro Gazzaneo