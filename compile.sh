#!/bin/bash
ant clean jar
tar -cf dist/voldemort-`git rev-parse HEAD`.tar dist/voldemort-0.90.1.jar dist/resources/* bin/* lib/*
