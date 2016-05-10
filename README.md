# Hive output plugin for Embulk

Hive Output Plugin for Embulk, write to HDFS and create external table to Hive.

attention: this plugin is developping, need to build yourself if you use.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

- **url**: URL of hiveserver2's JDBC connection (string, required)
- **user**: hive login user name (string, required)
- **password**: hive login user password (string, required)
- **database**: hive database name (string, required)
- **table**: hive external table name (string, required)
- **location**: data file's directory location of hive external table (string, required)
- **config_files**: configuration parameter files for hdfs (array of string, default: [])
- **config**: configuration parameter for hdfs (map of string, default: {})

## Example

```yaml
out:
  type: hive
  url: 'jdbc:hive2://hiveserver_host:10000/default'
  user: hive_user
  password: 'hive_password'
  database: default
  table: output_table
  location: /hive/externaltables/output_table
  config_files:
    - "{{ env.HADOOP_CONF_DIR }}/core-site.xml"
  config:
    fs.hdfs.impl: 'org.apache.hadoop.hdfs.DistributedFileSystem'
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## How To Use

this plugin is developping, need to build yourself by below procedure.

clone

```
git clone git@github.com:takemikami/embulk-output-hive.git
```

build

```
cd embulk-output-hive
gradle classpath
```

run

```
embulk run <your config.yaml here> -I <embulk-output-hive clone path here>/lib
```
