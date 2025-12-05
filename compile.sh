#!/usr/bin/env bash

# Clear console
clear

# Compile the Java code
javac -d out $(find src -name "*.java")

# Run the Broker Server
java -cp out Broker/BrokerServer
