RELEASE_TYPE: patch

Fixes a version mismatch with the hegel-core dependency that was causing the entire library to be broken.

Also improves error reporting to not silently swallow panics outside tests and to log server stdout to a file.
