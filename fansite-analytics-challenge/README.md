Hopefully this works. That is to say, it works, but I hope it runs in some unknown environment.
I did the development on Windows, then tested on a fresh AWS Ubuntu AMI on a t2.micro instance
and ran into numerous issues (installation, environment configuration, heap settings, etc). It
was not nearly as turn-key as I would have liked when my code is depending on a smooth set up.

Also, the Insight-provided test suite didn't work at all (I didn't bother debugging):
   1~/insight/fansite-analytics-challenge$ ./insight_testsuite/run_tests.sh
   -bash: ./insight_testsuite/run_tests.sh: /bin/bash^M: bad interpreter: No such file or directory
If that code isn't working out-of-the-box, it seems like an ominous sign for my code...

I wrote this in Scala because it seemed like a good idea initially. It gave me an excuse to try the
language for the very first time. And it is known to be popular in the data science world. So, it 
was worthwhile as a learning experience, but was probably not the best choice for demonstrating
programming ability. It would have been wiser to use a language I already had some experience with.

There are 2 extra features included, which are variants of feature 3. The primary implementation
of feature 3 follows the example given in the specification, where the hour time stamps correspond
to events. There is another implementation (feature3A) that aligns all 60 minute windows to the
beginning of each hour. The output from that implementation goes into alignedHours.txt. Then there
is another implementation (feature3B) that accumulates the number of accesses across days and
produces a list of 24 one hour time windows, which would show more macroscopic trends in the traffic.
The output of this other implementation goes into cumulativeHours.txt.

For what it's worth, the program takes two command line arguments. The first is the log file (ie.
./log_input/log.txt) and the second is the output directory (ie. ./log_output/). The output file
names are hardcoded.

The main gist of the code is that there's a publish/subscribe model going on. There is only one
thread being used and only one pass being made over the input file. Each feature is implemented as
a subscriber. There's minimal support for managing and prioritizing subscribers, which does not
get used in this demo.

--Tim O'Connor 4/5/17