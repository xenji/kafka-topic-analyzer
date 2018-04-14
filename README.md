# Kafka Topic Analyzer

[![Build Status](https://travis-ci.org/xenji/kafka-topic-analyzer.svg?branch=master)](https://travis-ci.org/xenji/kafka-topic-analyzer)

A CLI tool that gathers statistics about a Apache Kafka topic by reading
it from beginning to end and counting various metrics.

## Usage
    $> ./kafka-topic-analyzer -h
    Kafka Topic Analyzer

    USAGE:
        kafka-topic-analyzer --bootstrap-server <BOOTSTRAP_SERVER> --topic <TOPIC>

    FLAGS:
        -h, --help       Prints help information
        -V, --version    Prints version information

    OPTIONS:
        -b, --bootstrap-server <BOOTSTRAP_SERVER>    Bootstrap server(s) to work with, comma separated
        -t, --topic <TOPIC>                          The topic to analyze

## Example output
![demo_ouput.png]
