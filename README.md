# Hibernate OGM Contrb

*Version: 5.1.0.Final - 01-03-2017*

## Description

[Hibernate OGM](http://hibernate.org/ogm/) stores data in a NoSQL data
grid using the Hibernate ORM engine.

The benefits are fairly obvious:
 - write your model once using well known JPA annotations and select the right NoSQL data grid for your project
 - Hibernate is familiar to many people
 - you end up being able to use all the tools of the Hibernate ecosystem such as Hibernate Search or Hibernate Validator

Checkout <http://hibernate.org/ogm/> for more information.

This projects collection dialects for different datastores coming from contributors.

## Useful pointers

Latest Hibernate OGM Documentation:

 * Reference guide: <https://docs.jboss.org/hibernate/stable/ogm/reference/en-US/html_single/>
 * Additional content: <http://community.jboss.org/en/hibernate/ogm>

Bug Reports:

 * Hibernate JIRA (preferred): <https://hibernate.atlassian.net/projects/OGM/>
 * Mailing list: <hibernate-dev@lists.jboss.org>

Support:

 * The hibernate-ogm tag on Stackoverflow: <http://stackoverflow.com/questions/tagged/hibernate-ogm>
 * Our forum: <https://forum.hibernate.org/viewforum.php?f=31>

## Build instructions

The code is available on GitHub at <https://github.com/hibernate/hibernate-ogm-contrib>.

To run the full project build including tests for all backends, documentation etc. execute:

    mvn clean install -s settings-example.xml

To speed things up, there are several options for skipping parts of the build.
To run the minimum project build without integration tests, documentation and distribution execute:

    mvn clean install -DskipITs -DskipDocs -DskipDistro -s settings-example.xml

The following sections describe these options in more detail.

### Importing sources in Eclipse

Import the project as any standard Maven project.
This might trigger a dialog to automatically find and install additional m2e plugins: allow that.

Make sure that annotation processing is enabled in your project settings
(see "Properties" - "Maven" - "Annotation Processing",
the setting should be "Automatically configure JDT APT").

### Integration tests

You can skip integration tests by specifying the `skipITs` property:

    mvn clean install -DskipITs -s settings-example.xml

### Documentation

The documentation is built by default as part of the project build.
You can skip it by specifying the `skipDocs` property:

    mvn clean install -DskipDocs -s settings-example.xml

If you just want to build the documentation, run it from the _documentation/manual_ subdirectory.

By default, the following command only builds the HTML version of the documentation:

    mvn clean install -f documentation/manual/pom.xml -s settings-example.xml

If you also wish to generate the PDF version of the documentation,
you need to use the `documentation-pdf` profile:

    mvn clean install -f documentation/manual/pom.xml -s settings-example.xml -Pdocumentation-pdf

### Distribution

The distribution bundle is built by default as part of the project build. You can skip it by specifying the `skipDistro` property:

    mvn clean install -DskipDistro -s settings-example.xml

### Integration tests

Integration tests can be run from the integrationtest module and the default behaviour is to download the WildFly application server,
unpack the modules in it and run the tests using Arquillian.

#### WARNING
Be careful when using on existing installation since the modules used by the build are going to be extracted into the
server you want to run the test, changing the original setup.

### MongoDB

For executing the tests in the _mongodb_ and _integrationtest/mongodb_ modules, by default the
[embedmongo-maven-plugin](https://github.com/joelittlejohn/embedmongo-maven-plugin) is used which downloads the MongoDB
distribution, extracts it, starts a _mongod_ process and shuts it down after test execution.

If required, you can configure the port to which the MongoDB instance binds to (by default 27018)
and the target directory for the extracted binary (defaults to _${project.build.directory}/embeddedMongoDb/extracted_) like this:

    mvn clean install -s settings-example.xml -DembeddedMongoDbTempDir=<my-temp-dir> -DembeddedMongoDbPort=<my-port>

To work with a separately installed MongoDB instance instead, specify the property `-DmongodbProvider=external`:

    mvn clean install -s settings-example.xml -DmongodbProvider=external

This assumes MongoDB to be installed on `localhost`, using the default port and no authentication.
If you work with different settings, configure the required properties in hibernate.properties (for the tests in _mongodb_)
and/or the environment variables `MONGODB_HOSTNAME` `MONGODB_PORT` `MONGODB_USERNAME` `MONGODB_PASSWORD` (for the tests in _integrationtest/mongodb_)
prior to running the tests:

    export MONGODB_HOSTNAME=mongodb-machine
    export MONGODB_PORT=1234
    export MONGODB_USERNAME=someUsername
    export MONGODB_PASSWORD=someP@ssw0rd
    mvn clean install -s settings-example.xml -DmongodbProvider=external

Finally, you also can run the test suite against the in-memory "fake implementation" Fongo:

    mvn clean install -s settings-example.xml -DmongodbProvider=fongo

### CouchDB

For running the tests in the _couchdb_ module an installed CouchDB server is required. Specify its host name by
setting the environment variable `COUCHDB_HOSTNAME` prior to running the test suite:

    export COUCHDB_HOSTNAME=couchdb-machine

If this variable is not set, the _couchdb_ module still will be compiled and packaged but the tests will be skipped.
If needed, the port to connect to can be configured through the environment variable `COUCHDB_PORT`.

### Cassandra

For running the tests in the _cassandra_ module an installed Cassandra server is required. Specify its host name by
setting the environment variable `CASSANDRA_HOSTNAME` prior to running the test suite:

    export CASSANDRA_HOSTNAME=cassandra-machine

If this variable is not set, the _cassandra_ module still will be compiled and packaged but the tests will be skipped.
If needed, the port to connect to can be configured through the environment variable `CASSANDRA_PORT`.

### Redis

For running the tests in the _redis_ module an installed Redis server is required. Specify its host name by
setting the environment variable `REDIS_HOSTNAME` prior to running the test suite:

    export REDIS_HOSTNAME=redis-machine

If this variable is not set, the _redis_ module still will be compiled and packaged but the tests will be skipped.
If needed, the port to connect to can be configured through the environment variable `REDIS_PORT`.

Tests with the _redis_ module can be started using a Makefile. The Makefile takes care of downloading and compiling
a recent Redis version, starts a single Redis Standalone and four Redis Cluster nodes and can start the tests.

     make test # Make me happy and run tests against Redis Standalone and Redis Cluster
     make test-standalone
     make test-cluster

Commands to spin up/shut down the Redis instances:

    make start
    make stop

## Notes

If you want to contribute, come to the <hibernate-dev@lists.jboss.org> mailing list
or join us on #hibernate-dev on freenode (login required)

This software and its documentation are distributed under the terms of the
FSF Lesser Gnu Public License (see license.txt).
