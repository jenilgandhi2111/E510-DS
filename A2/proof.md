# E-510 Distributed Systems Assignment-2

### Totally Ordered Broadcast Proof

## Introduction:

For the totally ordered broadcast the objective is that all the messages should be processed in order on all processes. Also, we make an assumption that the delivery of the messages is FIFO (First in first out). That is the message that is generated and kept on the network is delivered first.

## Proof

### Assumption:

- The messages are delivered in FIFO (First in first out) order. Assuming there is no packet loss over the network, no congestion.
- We also assume none of the blocks go down during the execution of the program. That is none of the blocks get abruptly terminated.
- We assume that the application layer takes minimal time to process the message and the message at the top of the queue is dispatched directly without wait.
- We assume that a message only starts executing when it has recived all the ACK messages from other blocks and only when it is at the top the executing queue.

### Proof by contradiction:

- We assume that there are two processes say A and B running on two different blocks.
- Now we assume that A emits a message M1 at time Ta and B also emits a message M2 at Tb where Ta < Tb. Note: Ta and Tb are both lamport time of the messages.
- We assume that B is ready to be executed on the block B. This would mean that the Block B received all messages including the acknowledgement from the block A.
- So if B is ready to execute it means that the message A is lost over the network because of the assumption that each message that is broadcasted first is received first i.e FIFO ordering is violated because Ta < Tb.
- This contradicts the assumption we made of the FIFO ordering. Hence, we can say that B can never execute message B before processing messsage A messages are always delivered and the processing queue is always sorted based on timestamps.
- Adding to that ACK messages are only delivered when the message is at top of the processing queue. Hence there is no way a message with higher timestamp is executed before a message with lower timestamp.
