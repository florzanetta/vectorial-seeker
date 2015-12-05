Vectorial Search Engine
=======================

This is a proof of concept implementation of the vectorial search engine to
search for terms in a given set of files previously indexed and present the
results ordered by their relevance.

The project was built like a REST API using Netty framework. The user interface
consists in a single html file that you run from your own browser, because the
browser can't otherwise access the filesystem.
This was done this way as an experiment, the only purpose was to create the
search engine and a minimal user interface to use it, other than the REST API.

The gradle build script includes 2 database setup tasks to setup the database.
The database engine used in this project is MySQL, but it doesn't take much
to change it to use any DB you like.
