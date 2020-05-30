# Docker for Dev

**Usage:**

Run:

    docker-compose run --rm integration_tests

Environment variables can be set before `docker-compose run`:

    export MONGO_PROFILE=invalid-ssl

> See matrix in the [CI configuration](../../.travis.yml)

It's also possible to obtain an interactive shell from the test container.

    docker-compose run --rm --entrypoint /bin/ash integration_tests

In the interactive shell, you can execute `./.ci_scripts/runIntegration.sh interactive`

To check the MongoDB logs:

    docker logs -f reactivemongo_db_...

To only run the MongoDB with a test profile (e.g. `rs`):

     MONGO_PROFILE=rs docker-compose -f docker-compose.yml up

Cleanup:

```
docker-compose stop
docker-compose rm
```
