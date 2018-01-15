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

    sbt "testOnly -- exclude mongo2"

When running against MongoDB 2.6, the command must replace `exclude mongo2` with `exclude not_mongo26`.

> The test environement must be able to handle the maximum number of incoming connection for the MongoDB instance. This must be checked, and eventually updated, using `ulimit -n`.

[Travis](https://travis-ci.org/ReactiveMongo/ReactiveMongo): ![Travis build status](https://travis-ci.org/ReactiveMongo/ReactiveMongo.png?branch=master)
[![Test coverage](https://img.shields.io/badge/coverage-60%25-yellowgreen.svg)](https://reactivemongo.github.io/ReactiveMongo/coverage/0.12.7/)

*Reproduce Travis build:*

To reproduce a Travis build, the following script can be used to setup a local test environment.

```
# According travis `env`
export CI_CATEGORY=INTEGRATION_TESTS \
       AKKA_VERSION=2.5.6 \
       ITERATEES_VERSION=2.6.1 \
       MONGO_PROFILE=invalid-ssl \
       MONGO_VER=3_4 \
       ENV_FILE="/tmp/integration-env.sh"

# Settings to start mongod (available in local PATH)
# !! Make sure not have any other services already bound on the ports
export MONGO_MINOR="3.4.10" \
       PRIMARY_HOST="localhost:27018" \
       PRIMARY_SLOW_PROXY="localhost:27019"

./.ci_scripts/setupEnv.sh $MONGO_VER $MONGO_MINOR $MONGO_PROFILE \
  $PRIMARY_HOST $PRIMARY_SLOW_PROXY "$ENV_FILE"

if [ "x$CI_CATEGORY" = "xINTEGRATION_TESTS" ]; then ( \
  ./.ci_scripts/fork-mongod.sh /tmp/integration-env.sh); fi
```

Once the local environment is set up, the test build can be executed using the following commands.

```
export SCALA_VERSION=2.12.4

./.ci_scripts/validate.sh /tmp/integration-env.sh
```

It's possible to restrict the executed tests as below.

```
export TEST_CLASSES=UpdateSpec

# Only run the `UpdateSpec` test suite
./.ci_scripts/validate.sh /tmp/integration-env.sh

export TEST_CLASSES="UpdateSpec DriverSpec CursorSpec"
./.ci_scripts/validate.sh /tmp/integration-env.sh
```

### Learn More

- [Complete documentation and tutorials](http://reactivemongo.org)
- [Search or create issues](https://github.com/ReactiveMongo/ReactiveMongo/issues)
- [Get help](https://groups.google.com/forum/?fromgroups#!forum/reactivemongo)
- [Contribute](https://github.com/ReactiveMongo/ReactiveMongo/blob/master/CONTRIBUTING.md#reactivemongo-developer--contributor-guidelines)

**Samples:**  

These sample applications are kept up to date with the latest driver version. They are built upon Play 2.6.

* [A simple TODO app built with Play, Swagger and ReactiveMongo](https://github.com/ricsirigu/play26-swagger-reactivemongo)

These sample applications are built upon Play 2.3.

* [ReactiveMongo Tailable Cursor, WebSocket and Play 2](https://github.com/sgodbillon/reactivemongo-tailablecursor-demo)
* [Full Web Application featuring basic CRUD operations and GridFS streaming](https://github.com/sgodbillon/reactivemongo-demo-app)
