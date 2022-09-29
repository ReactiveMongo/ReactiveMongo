---
name: Driver issue or feature request
about: Confirmed driver issue or actionable feature request
title: ''
labels: ''
assignees: ''

---

### Are you looking for help?

This tracker is not a support system and so it is not a place to ask questions or get help. If you're not sure if you have found a bug, the best place to start is with either the [users mailing list](https://groups.google.com/forum/?fromgroups#!forum/reactivemongo).

This tracker is for confirmed issue or actionable feature request, used to manage and track the development of ReactiveMongo.

### ReactiveMongo Version (0.10.5, 0.11.6 / etc)


### MongoDB version (2.6, 3.0 / etc)

### Operating System (Ubuntu 15.10 / MacOS 10.10 / Windows 10)

> Use `uname -a` if on Linux.

### JDK (Oracle 1.8.0_72, OpenJDK 1.8.x, Azul Zing)

> Paste the output from `java -version` at the command line.

### Library Dependencies

If this is an issue that involves integration with other frameworks (e.g. Playframework), include the exact versions the other frameworks.

### Expected Behavior

Please describe the expected behavior of the issue, starting from the first action.

1.
2.
3.

### Actual Behavior

Please provide a description of what actually happens, working from the same starting point.

Be descriptive: "it doesn't work" does not describe what the behaviour actually is -- instead, say "BSONCollection.drop() fails with 'command drop failed because the 'ok' field is missing or equals 0'"  Copy and paste logs, and include any URLs. Turn on internal SLF4J logging if there is no log output (e.g. with Play/logback logging `<logger name="reactivemongo" value="TRACE"/>`).

1.
2.
3.

### Reproducible Test Case

Please provide a PR with a failing test.  

If the issue is more complex or requires configuration, please provide a link to a project on Github that reproduces the issue.
