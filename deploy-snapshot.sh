#!/bin/bash

mvn -e -Dmaven.test.skip=true -DaltDeploymentRepository=$MVN_SNAPSHOT_REPO clean install deploy

