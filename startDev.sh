#!/bin/bash
nodemon -e java -w ./src -x 'mvn clean compile exec:java -Dexec.mainClass=com.customcameltosolr.camel.App2'
