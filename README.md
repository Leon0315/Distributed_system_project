# Distributed_system_project
This project explored a primary/backup replication mechanism for a database system. The system consists a viewserver and a primary/backup service 

In this project, we explored a primary/backup replication mechanism for a database system. The motivation was based on the flaw of the single system, in which the main server could fail and the client would lose access to resources. In our design, we introduced a third component viewserver to monitor the availability of all servers in the network and made a replica from the master server. In an event of server failure, the viewserver would appoint backups to replace the failed server and keep all otherservers informed of the change.

A viewservice is responsible for monitoring the primary/backup and all other idle servers.
It will establish a primary and backup server by responding to ping requests.

The primary/backup service handles the client request on database and return the value to the client. It uses the viewserver to determine the primary server and to replace failed server.

Below is a diagram to show the overall system structure.

![Screenshot](system_stru.png) 

Below is a how the data flow in Clerk.Get(key) / Clerk.Put(key/value) / Clerk.Append(key/value) insides the PBServer. It shows how the primary and backup server communicate with each other and syncronize the its database.

![Screenshot](data_flow.png) 


# How to test
You can test the function of viewserver and P/B service independently. 

[Run viewserver test]:

Navigate ~ src/viewservice/
```go test```


[Test1. First Primary]

[Test2. First Backup]

[Test3. Backup takes over if primary fails]

[Test4. Restarted server becomes backup]

[Test5. Idle third server becomes backup if primary fails]

[Test6. Restarted primary treated as dead]

[Test7. Dead backup is removed from view]

[Test8. Viewserver waits for primary to ack view]

[Test9. Uninitialized server can't become primary]

~ src/pbservice/
```go test```
