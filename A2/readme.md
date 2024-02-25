# Distributed Systems Assignment 2

## How to test

- To check if the ports required for the testing are free.
  - If on a Linux system run the file _killports.sh_ and provide the ports in the port.txt file as a command line argument in a string example usage: ./killports.sh "3000 3001".
  - If on a Windows system run the file same as Linux but use the file _killport.bat_.
- After we kill the ports remove the files inside the outputs folder or ensure the files are empty.
- Now run the file Runner.py in the main folder. Run it and the processing order can be observed in the output folder where the file name x.txt where x represents a process say 1.txt would mean the order in which the block 1 processed the data.

## Example:

```
1.) Application processesA1 On block ID:1
2.) Application processesA2 On block ID:1
3.) Application processesA3 On block ID:1
```

This shows that A1 is processed first and A2 after it.
