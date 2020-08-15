# Distributed_system_project
This project explored a primary/backup replication mechanism for a database system. The system consists a viewserver and a primary/backup service 

A viewservice is responsible for monitoring the primary/backup and all other idle servers.
It will establish a primary and backup server by responding to ping requests.

The primary/backup service handles the client request on database and return the value to the client. It uses the viewserver to determine the primary server and to replace failed server.

Below is a diagram to show the overall system structure.
![Screenshot](system_stru.png) 

![Screenshot](PBServer_Data_Flow.png) 


# How to test
You can test the function of viewserver and P/B service independently. 

Navigate ~ src/viewservice/
```go test```

~ src/pbservice/
```go test```
