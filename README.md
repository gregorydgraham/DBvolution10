# DBvolution
Advanced Library to Remove Object Relational Impedance. 

DBvolution is a java library that reduces SQL access to simple java calls.

DBvolution translates all database concepts into Object Oriented concepts, allowing you to spend all your time writing Java rather than fixing broken and awkward SQL.

DBvolution transforms your schema into classes, reduces the database configuration to sparse annotations on the classes, and allows querying directly from the classes.

Queries are created inside your java code and takes as little as one line for a multi-table outer join. Retrieving the rows from the query is only one more method call. Dozens of SQL functions are available without leaving your Java code and use chaining to build complex expressions easily.

Transactions are encapsulated into a thread-like API, allowing you to write complex database interactions in complete safety.

The actions performed by DBvolution are always available for debugging and checking by DBAs before release.

There a demonstration project at https://github.com/gregorydgraham/DBvolution-Demo, examples in http://nz.co.gregs.dbvolution.examples, and documentation at http://dbvolution.gregs.co.nz
