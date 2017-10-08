# ReactiveMongo

ReactiveMongo is a scala driver that provides fully non-blocking and asynchronous I/O operations.

## Usage

In your `project/Build.scala`:

```scala
libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "VERSION"
)
```

[![Maven](https://img.shields.io/maven-central/v/org.reactivemongo/reactivemongo_2.12.svg)](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22reactivemongo_2.12%22) [![Javadocs](https://javadoc.io/badge/org.reactivemongo/reactivemongo_2.12.svg)](https://javadoc.io/doc/org.reactivemongo/reactivemongo_2.12)

## Build manually

To benefit from the latest improvements and fixes, you may want to compile ReactiveMongo from source. You will need a [Git client](http://git-scm.com/) and [SBT](http://www.scala-sbt.org).

From the shell, first checkout the source:

```
$ git clone git@github.com:ReactiveMongo/ReactiveMongo.git
```

Then go to the `ReactiveMongo` directory and launch the SBT build console:

```
$ cd ReactiveMongo
$ sbt
> +publish-local
```

*Running tests:*

In order to execute the unit and integration tests, SBT can be used as follows.

    sbt "test-only -- exclude mongo2"

When running against MongoDB 2.6, the command must replace `exclude mongo2` with `exclude not_mongo26`.

> The test environement must be able to handle the maximum number of incoming connection for the MongoDB instance. This must be checked, and eventually updated, using `ulimit -n`.

[Travis](https://travis-ci.org/ReactiveMongo/ReactiveMongo): ![Travis build status](https://travis-ci.org/ReactiveMongo/ReactiveMongo.png?branch=master)
[![Test coverage](https://img.shields.io/badge/coverage-60%25-yellowgreen.svg)](https://reactivemongo.github.io/ReactiveMongo/coverage/0.12.0/)

### Learn More

- [Complete documentation and tutorials](http://reactivemongo.org)
- [Search or create issues](https://github.com/ReactiveMongo/ReactiveMongo/issues)
- [Get help](https://groups.google.com/forum/?fromgroups#!forum/reactivemongo)
- [Contribute](https://github.com/ReactiveMongo/ReactiveMongo/blob/master/CONTRIBUTING.md#reactivemongo-developer--contributor-guidelines)

**Samples:** These sample applications are kept up to date with the latest driver version. They are built upon Play 2.3.

* [ReactiveMongo Tailable Cursor, WebSocket and Play 2](https://github.com/sgodbillon/reactivemongo-tailablecursor-demo)
* [Full Web Application featuring basic CRUD operations and GridFS streaming](https://github.com/sgodbillon/reactivemongo-demo-app)
