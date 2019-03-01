---
title: Manual
layout: home
---

easy-bag-index
==============
[![Build Status](https://travis-ci.org/DANS-KNAW/easy-bag-index.png?branch=master)](https://travis-ci.org/DANS-KNAW/easy-bag-index)

Index a bag store

SYNOPSIS
--------

    easy-bag-index index [--force | -f] <item-id> # Retrieves the information from the cache or directly from the bag-store
    easy-bag-index run-service # Runs the program as a service

DESCRIPTION
-----------
Provides an interface to add and retrieve relational metadata of individual bags from the index.

ARGUMENTS
---------

    Options:

    -h, --help      Show help message
    -v, --version   Show version of this program

    Subcommand: index - Starts EASY bag index as a daemon that services HTTP requests
        -f, --force  Force the indexing without asking for confirmation
        -h, --help   Show help message

    trailing arguments:
           item-id (required)

    ---

    Subcommand: run-service - Starts EASY bag index as a daemon that services HTTP requests
         -h, --help   Show help message

EXAMPLES
--------

```jshelllanguage
easy-bag-index index 40594b6d-8378-4260-b96b-13b57beadf7c
```

```json
{
  "result":{
    "bag-info":{
      "bag-id":"40594b6d-8378-4260-b96b-13b57beadf7c",
      "base-id":"40594b6d-8378-4260-b96b-13b57beadf7c",
      "created":"2015-05-19T00:00:00.000+02:00",
      "doi":"10.5072/dans-x6f-kf66"
    }
  }
}

```

INSTALLATION AND CONFIGURATION
------------------------------

### Steps

1. Unzip the tarball to a directory of your choice, typically `/usr/local/`
2. A new directory called easy-bag-index-<version> will be created
3. Add the command script to your `PATH` environment variable by creating a symbolic link to it from a directory that is
   on the path, e.g.

        ln -s /usr/local/easy-bag-index-<version>/bin/easy-bag-index /usr/bin


General configuration settings can be set in `cfg/application.properties` and logging can be configured
in `cfg/logback.xml`. The available settings are explained in comments in aforementioned files.



BUILDING FROM SOURCE
--------------------

Prerequisites:

* Java 8 or higher
* Maven 3.3.3 or higher

Steps:

        git clone https://github.com/DANS-KNAW/easy-bag-index.git
        cd easy-bag-index
        mvn install
