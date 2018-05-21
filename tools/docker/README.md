# Docker for Dev

**Usage:**

Run:

    docker-compose run --rm integration_tests

Environment variables can be set before `docker-compose run`:

    export MONGO_PROFILE=invalid-ssl

> See matrix in the [CI configuration](../../.travis.yml)

It's also possible to obtain an interactive shell from the test container.

    docker-compose run --rm --entrypoint /bin/ash integration_tests

Cleanup:

```
docker-compose stop
docker-compose rm
```
