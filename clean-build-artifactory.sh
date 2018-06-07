#!/bin/bash

mvn clean install deploy -DskipTests -DaltDeploymentRepository=snapshots::default::https://rhartifactory.jfrog.io/rhartifactory/libs-snapshot-local
