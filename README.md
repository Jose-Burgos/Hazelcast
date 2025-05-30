# TPE2 Reclamos Urbanos – Distributed Complaint Analysis with MapReduce and Hazelcast

This project is the second special practical assignment for the Concurrent Programming course at ITBA (Instituto Tecnológico de Buenos Aires).

The goal is to design and implement a distributed console application using the MapReduce paradigm and the Hazelcast framework to process
and analyze large-scale urban complaint datasets from New York City (NYC) and Chicago (CHI).

---

## Features
- Distributed processing of 311 service request datasets (up to 30M+ records)
- Modular Maven project with separation between client, server, and api
- - Support for both NYC and CHI complaint formats
- Implementation of 5 distinct analytical queries using MapReduce:
1. Total complaints by type and agency
2. Most common complaint by neighborhood and geo quadrant
3. Moving average of open complaints by agency and month
4. Percentage of complaint types per street in a neighborhood

---

## Technologies Used

- Java 17
- Hazelcast 3.8.6
- MapReduce
- Apache Maven
- JUnit (for testing)
- Java NIO for CSV file parsing

---

## Project Structure
```
tpe2-g12-parent/
├── tpe2-g12-api/       # Shared models and interfaces
├── tpe2-g12-client/    # Console applications for each query
├── tpe2-g12-server/    # Hazelcast job workers and reducers
└── README.md           # Setup and usage instructions
```
---

## How to Run

Each query can be executed individually using shell scripts (e.g. query1.sh) with the following syntax:
```
bash
sh queryx.sh 
    -Daddresses='10.6.0.1:5701;10.6.0.2:5701' 
    -Dcity={NYC|CHI} 
    -DinPath=/path/to/input 
    -DoutPath=/path/to/output
    [params]
```

## Authors
This project was developed by Group 12 as part of the Distributed Object Programming course at ITBA:

- Jose Burgos
- Pedro Curti
- Lautaro Gazzaneo