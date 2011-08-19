#!/bin/bash

MVN_RELEASE_REPO=releases::default::file:../dlyubimov-maven-repo/releases    
MVN_SNAPSHOT_REPO=snapshots::default::file:../dlyubimov-maven-repo/snapshots

mvn -e -Dmaven.test.skip=true -DaltDeploymentRepository=$MVN_SNAPSHOT_REPO clean install deploy

