#!/bin/sh

scalac ./src/insightProgrammingChallenge.scala
#I don't like the idea of manually tinkering with the heap size.
#But, this ran successfully on  a Windows machine using the defaults,
#then it crashed because it ran out of heap on a fresh AWS Ubuntu AMI on a t2.micro instance.
scala -J-Xmx1g insightProgrammingChallenge ./log_input/log.txt ./log_output/
