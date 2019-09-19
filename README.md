# CRUCIAL
_Stateful Distributed Applications over Serverless Architectures_

Function-as-a-Service (FaaS) enables programmers to develop cloud applications
as individual functions that can run and scale independently.
However, storage and compute resources must be separated in FaaS.
Applications that require support for mutable state and synchronization,
such as machine learning and scientific computing, are hard to build.

Crucial is a system to program highly-concurrent stateful applications on
serverless architectures. It keeps a simple programming model and allows to
port effortlessly multi-threaded algorithms to FaaS.

## Programming

Crucial associates each thread in a multi-threaded application with a FaaS
function invocation. We call this new form of thread _cloud thread_.

With our abstraction 
[here](https://github.com/danielBCN/crucial-examples/tree/master/aws-executor),
a regular Java thread creation such as
```java
Thread t = new Thread(runnable);
```
can be run as a cloud function with a simple change:
```java
Thread t = new CloudThread(runnable);
```

To remotely share objects, Crucial maintains them in a separated in-memory
layer. Shared objects are made so by annotating their declaration with
```java
@Shared(key="counter")
Long counter = new Long(0);
```
These objects are wait-free and linearizable, meaning that concurrent method
invocations behave as if they were executed by a single thread.

You can find application examples in our
[examples repository](https://github.com/danielBCN/crucial-examples).

## Architecture

![arch](./images/architecture.png "Crucial architecture")

Crucial encompasses three main components:

 1. the FaaS computing layer that runs the cloud threads
 2. the DSO layer (distributed shared object) that stores the shared objects
 3. the client application

The client application creates and manages the execution of cloud threads, and
all of them access the the shared data using the DSO layer.

## Usage

### Application and cloud threads
Applications are built like simple Java code that uses Crucial's abstractions.
See [this repository](https://github.com/danielBCN/crucial-examples)
for examples.

### DSO layer
Crucial's DSO layer (this repository) relies on a client-server model.
It is built atop [Infinispan](https://infinispan.org).
To build the project, use `maven package` at the root of the project.
Since the applications will need the client, you may want to install it
(`maven install`).
The server will be contained in an archive file at `server/target/server-*.tar.gz`.
Then, to launch the server, uncompress and run the script `server.sh` where
desired.

Every class used by a client should be known at the server.
This requires to add the appropriates `.class` or `jar` files to the classpath
of the server. The server can dynamically load new jars located at a specified
directory (by default, it loads from `/tmp`).

## Crucial DSO layer at Amazon EC2 (cluster)


## Crucial white paper
