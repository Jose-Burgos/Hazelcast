# Urban Complaints Processing with Hazelcast MapReduce

This project implements a distributed processing solution for urban complaints using **Hazelcast 3.8.6** and **Java**. The solution processes large volumes of complaint data from New York City (NYC) and Chicago (CHI), applying specific queries to generate useful statistics.

## Table of Contents

- [Requirements](#requirements)
- [Project Structure](#project-structure)
- [Implemented Queries](#implemented-queries)
    - [Query 1: Complaint Counts by Type and Agency](#query-1-complaint-counts-by-type-and-agency)
    - [Query 2: Most Popular Type by Neighborhood and Grid](#query-2-most-popular-type-by-neighborhood-and-grid)
    - [Query 3: Moving Average of Open Complaints by Agency](#query-3-moving-average-of-open-complaints-by-agency)
    - [Query 4: Percentage of Complaint Types by Street in a Neighborhood](#query-4-percentage-of-complaint-types-by-street-in-a-neighborhood)
- [Compilation](#compilation)
- [Technical Notes](#technical-notes)
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

## Implemented Queries

### Query 1: Complaint Counts by Type and Agency

- **Description:** Calculates the total number of complaints by type and agency.
- **Execution Command:**
  ```
  java -Daddresses=<hazelcast_ip:port> -Dcity=NYC -DinPath=./data -DoutPath=./output -cp target/your-jar.jar hazelcast.client.q1.Query1Client
  ```

### Query 2: Most Popular Type by Neighborhood and Grid

- **Description:** Calculates the most frequent complaint type by neighborhood and grid cell.
- **Additional required parameter:** `-Dq=<grid cell size in degrees>`
- **Execution Command:**
  ```
  java -Daddresses=<hazelcast_ip:port> -Dcity=CHI -DinPath=./data -DoutPath=./output -Dq=0.1 -cp target/your-jar.jar hazelcast.client.q2.Query2Client
  ```

### Query 3: Moving Average of Open Complaints by Agency

- **Description:** Calculates the moving average of open complaints by agency, year, and month, using a window of `w` months.
- **Additional required parameter:** `-Dw=<window size>`
- **Execution Command:**
  ```
  java -Daddresses=<hazelcast_ip:port> -Dcity=NYC -DinPath=./data -DoutPath=./output -Dw=3 -cp target/your-jar.jar hazelcast.client.q3.Query3Client
  ```

### Query 4: Percentage of Complaint Types by Street in a Neighborhood

- **Description:** Calculates the percentage of different complaint types by street within a specific neighborhood.
- **Additional required parameter:** `-Dneighbourhood=<neighborhood_name>` (use `_` instead of spaces)
- **Execution Command:**
  ```
  java -Daddresses=<hazelcast_ip:port> -Dcity=NYC -DinPath=./data -DoutPath=./output -Dneighbourhood=STATEN_ISLAND -cp target/your-jar.jar hazelcast.client.q4.Query4Client
  ```

## Compilation

Compile the project using Maven:

```
mvn clean package
```

This will generate the JAR file in the `target` folder.

## Technical Notes

- The processing is based on Hazelcast's pure MapReduce.
- Utility classes like `Pair` and `CountPair` are used for keys and values in the Mappers and Reducers.
- Dates and percentages are carefully handled using `java.time` and `DecimalFormat` with `RoundingMode`.
- The solution can handle datasets with millions of complaints thanks to Hazelcast's distributed processing.

## Author
- Jose Burgos
- Pedro Curti
- Lautaro Gazzaneo