#!/bin/bash

javac -d bin -cp bin src/uga/cs4370/mydbimpl/RAimpl.java

javac -d bin -cp bin src/uga/cs4370/mydbimpl/Driver.java
java -cp bin uga.cs4370.mydbimpl.Driver 
