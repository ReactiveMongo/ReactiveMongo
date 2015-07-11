# ReactiveMongo

ReactiveMongo is a scala driver that provides fully non-blocking and asynchronous I/O operations.

## Usage

In your `project/Build.scala`:

```scala
libraryDependencies ++= Seq(
  "org.reactivemongo" %% "reactivemongo" % "VERSION"
)
```

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.reactivemongo/reactivemongo_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.reactivemongo/reactivemongo_2.11/)

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

[Travis](https://travis-ci.org/ReactiveMongo/ReactiveMongo): ![Travis build status](https://travis-ci.org/ReactiveMongo/ReactiveMongo.png?branch=master)

### Learn More

- [Complete documentation and tutorials](http://reactivemongo.org)
- [Search or create issues](https://github.com/ReactiveMongo/ReactiveMongo/issues)
- [Get help](https://groups.google.com/forum/?fromgroups#!forum/reactivemongo)
- [Contribute](https://github.com/ReactiveMongo/ReactiveMongo/blob/master/CONTRIBUTING.md#reactivemongo-developer--contributor-guidelines)

**Samples:** These sample applications are kept up to date with the latest driver version. They are built upon Play 2.3.

* [ReactiveMongo Tailable Cursor, WebSocket and Play 2](https://github.com/sgodbillon/reactivemongo-tailablecursor-demo)
* [Full Web Application featuring basic CRUD operations and GridFS streaming](https://github.com/sgodbillon/reactivemongo-demo-app)
