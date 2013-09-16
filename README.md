MongoDBConnector
================
This project shows how to create a connector to one or several MongoDB collections with moTwin Platform 3.2. 

This project contains an AbstractMongoDBSource which implements the basic behavior to get connected to a Mongo database. It defines the abstract methods in order to implement in subclasses how to perform the query, and how to map the query result to a virtual table row. Have a look at MyTableSource to have an example of how this can be implemented.

The mongodb.properties file must be updated to set the connections parameters to your Mongo database (host, port, and database name). This file must be deployed on the MOS runtime before you deploy the MongoDBConnector application.

