
# 2PL tester

Script checking whether or not a schedule of operations on a database respects:
 - 2PL protocol, plain, strict and strong
 - Conflict serializability
 - Concurrency using timestamps
 
Useful for the course of Data Managment of Maurizio Lenzerini @ Sapienza. 


## How to use

The software was put behind a minimal front-end, now running [here](https://two-pl-scheduler.appspot.com/2PL)

If, for whatever reason, the link is down clone this repo, `cd` into `src/` and launch `main.py`. Flask is required.


## Caveats

Some schedules raises an error inside the code. If you see "Internal server error" feel free to open an issue posting the input that made the system crash.


