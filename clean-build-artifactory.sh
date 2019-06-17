#!/bin/bash

# mvn clean package deploy -DaltDeploymentRepository=snapshots::default::https://rhartifactory.jfrog.io/rhartifactory/libs-snapshot-local
mvn clean package deploy -DaltDeploymentRepository=snapshots::default::https://rhartifactory.jfrog.io/rhartifactory/libs-release-local
