# ReactiveMongo

ReactiveMongo is a scala driver that provides fully non-blocking and asynchronous I/O operations.

## Usage

First add the dependencies in your `build.sbt`.

```scala
libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "VERSION"
)
```

[![Maven](https://img.shields.io/maven-central/v/org.reactivemongo/reactivemongo_2.12.svg)](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22reactivemongo_2.12%22) [![Javadocs](https://javadoc.io/badge/org.reactivemongo/reactivemongo_2.12.svg)](https://javadoc.io/doc/org.reactivemongo/reactivemongo_2.12)

*See the [documentation](http://reactivemongo.org)*

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
> +publishLocal
```

*Running tests:*

In order to execute the unit and integration tests, SBT can be used as follows.

    sbt testOnly

> The test environement must be able to handle the maximum number of incoming connection for the MongoDB instance. This must be checked, and eventually updated, using `ulimit -n`.

[![CircleCI](https://circleci.com/gh/ReactiveMongo/ReactiveMongo.svg?style=svg)](https://circleci.com/gh/ReactiveMongo/ReactiveMongo)
[![Test coverage](https://img.shields.io/badge/coverage-60%25-yellowgreen.svg)](https://reactivemongo.github.io/ReactiveMongo/coverage/0.12.7/)

*Reproduce CI build:*

To reproduce a CI build, see the [Docker](tools/docker/README.md) tools.

### Learn More

- [Complete documentation and tutorials](http://reactivemongo.org)
- [Search or create issues](https://github.com/ReactiveMongo/ReactiveMongo/issues)
- [Get help](https://groups.google.com/forum/?fromgroups#!forum/reactivemongo)
- [Contribute](https://github.com/ReactiveMongo/ReactiveMongo/blob/master/CONTRIBUTING.md#reactivemongo-developer--contributor-guidelines)

*See also the [samples](http://reactivemongo.org/#samples)*
