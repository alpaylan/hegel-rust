RELEASE_TYPE: patch

This release changes the way the client manages the server to run a single persistent process for the whole test run.

This should improve the performance of running many hegel tests, and also hopefully fixes an intermittent hang we would sometimes see when many hegel tests were run concurrently.
