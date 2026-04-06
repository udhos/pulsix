
Clarifications on the design of `inject`:

1. Retry behavior on hard-fail

A: The package should keep performing all the abstract consumer logic internally until the end of the input channel. Including moving Unsent to Unacked, Unacked to Acked, and reverting Unacked to Unsent on ack.Err. We are doing nothing new, just factoring out the consuming logic from `pulsix-ingress-random`.

2. Callback signature
"Report the message was reliably sent" — is the callback called once per message or once per ack batch? What does it receive? The Ack.AckedUpTo only gives a watermark ID, not the original message.

A: The ackedupto is an internal tracking for messages the `inject` package forwarded to reliable storage. The callback is a separate mechanism that the `inject` package calls to report to the caller writing to input channel that one specific message was reliably sent. I think the input channel should deliver us a message format that includes a receipt we should use to invoke the callback with to signal the message was reliably sent.

3. Sender ownership
Does inject construct the *pub.Sender internally (taking pub.SendOptions), or does the caller pass a pre-built one? "Builds on pub.Sender" could mean either.

A: I think we should start by taking a pre-built *pub.Sender, because it would simplify the inject package.

4. Lifecycle / shutdown
How does inject stop? Candidates: close the input channel, cancel a context.Context, or call a Stop()/Close() method. And who calls sender.Close()?

A: Ideally it only stops at the end of the input channel, if that ever happens.

5. Goroutine model
Does the caller invoke a blocking Run(ctx context.Context) error, or does inject spawn goroutines internally and return control immediately?

A: First shot: inj := inject.New(...), then inj.Run() blocks until the input channel is closed and all messages are processed.

6. High: Cancellation semantics are still ambiguous.

A: Remove the context cancellation for now.

7. High: Delivery guarantee of the callback needs one explicit rule.

A: The rule is, once we move a message from Unacked to Acked, we call the callback with the message receipt.

8. Medium: Run error contract is not defined.

A: For now we run forever until the input channel is closed. If we find any fatal error we should handle in other way, we must return to this question.

9. Medium: Sender ownership is mostly clear but one sentence is missing.
You say caller passes pre-built sender. Also state who closes it.

A: Since we run forever and the caller blocks on us, they can close the sender if we ever return.

10. Medium: Input item shape should be concrete.
You mention message plus receipt. Define it as a named struct with fields and semantics for receipt identity (opaque token). This keeps API stable for Kafka/RabbitMQ/SQS adapters.

A: This is just a suggestion, we can revise it if needed.

```golang
type InjectMessage struct {
  Receipt string // opaque token for the caller to identify the message
  Data []byte // the actual message data to be sent to Pulsix
}
```

11. Low: Ordering and throughput expectations are unstated.
Specify whether callbacks preserve send order or may be out of order due to ack watermark advancement.

There is no constraint on order. We notify as we move messages from Unacked to Acked.

12. Also mention backpressure behavior when input is faster than sender progress.

A: KISS. Buffered channel.

13. High: Circular shutdown ownership is still a blocker.
You currently say Run blocks until input channel closes, and caller closes sender only if Run returns. That can deadlock because sender close is typically needed to flush and close ack flow before Run can finish.

A: The caller both (a) blocks on inject.Run and (b) feeds the channel. It is likely the caller needs at least two goroutines for that. Eventually the caller might finish the data stream by closing the channel. Then the inject.Run(...) could finish processing and return. The hard contract is: inject.Run(...) reads the channel forever unless it is closed. If it is closed, it might return if we finish our internal work.

14. Medium: Run forever wording conflicts with a finite completion contract.
You say run forever until input closes, but also “all messages are processed” and “fatal error question later.” This leaves no current behavior for unexpected sender failures or panic-safe exit.

A: Reinforcing the real contract. We read the input channel while it is open. That is the meaning of "run forever".

15. Medium: Backpressure is underspecified in terms of responsibility.
“KISS. Buffered channel.” is directionally fine, but it does not say who allocates capacity and what happens when full.

A: inject.New(...) creates an object with a buffered channel like `C` that the caller can send into.

16. Low: Input message shape is marked as “just a suggestion,” which weakens implementation readiness.
If this is your intended first API, treat it as the concrete v1 type to avoid churn during implementation.

A: The suggestion holds. Use it until I ask otherwise or you find it unfitting.

17. High: Shutdown ownership is still internally inconsistent and can deadlock.
In README.md:444 you say caller closes sender only if Run returns, but in README.md:469 Run returns only after internal work finishes after channel close. With pub.Sender, finishing cleanly usually requires sender.Close() to flush final batch and terminate ack flow.
Required fix: state that inject.Run() calls sender.Close() itself after input channel close and after it has drained/processed acknowledgments.

A: The caller provides the sender. Thus, the caller shuts it down. Inject task is only to drain the input channel and perform the abstract producer state machine.

18. Medium: Channel ownership now conflicts with the original “two inputs” contract.
The todo says inject should take a message channel + callback as inputs (README.md:392), but later you switch to inject.New(...) creating its own buffered channel C (README.md:474).
Required fix: pick one v1 API shape and keep it consistent. Either:
New(sender, in <-chan InjectMessage, onAck) (caller owns channel), or
New(sender, onAck, buffer) exposing inj.C (inject owns channel).

A: The take for message channel was a loose wording for specifying the two required parameters (the sender and the callback). Later we decided on inject.New() would create the channel.

19. Medium: Callback type is still not concrete enough for implementation.
You define InjectMessage, but not the callback signature. A concrete v1 signature avoids ambiguity around error handling and blocking behavior.
Suggested v1: type AckFunc func(receipt string) and document it must be fast/non-blocking or it can stall progress.

A: func callback(receipt string) is the v1 signature. We can revise it if we find it unfitting.

20. Low: Return contract can be minimal but should be explicit in one sentence.
Current wording says “run forever while channel open” (README.md:471), but still leaves return semantics implicit.

A: Run forever is indeed ambiguous. It was a simplification for read the channel forever while it is open.

21. High: Sender shutdown sequence is still not explicitly safe in one sentence.
You now clearly want caller-owned sender lifecycle, which is fine, but the spec should state the exact sequence: close input channel, wait for Run to return, then caller calls sender.Close. Without that explicit order in README.md:431, implementers may close sender too early and trigger send failures during drain.

A: Only the caller close the channel, when he wants. Only the caller closes the sender, when he wants. We as inject do not care about that. We just read the channel while it is open.

22. Medium: Run error behavior is still intentionally open-ended.
You say fatal errors will be addressed later in README.md:439. That is acceptable for experimentation, but implementation still needs a temporary rule now (for example: return first fatal error from sender.Send or callback panic). Otherwise tests cannot assert behavior.

A: I do not foresee any possible fatal error for now. If you find one during experimentation, log as slog.Error("DESIGN ERROR: please look at this fatal error closer")

23. Low: API shape is now mostly clear but should be stated as final v1 text once.
The final direction appears to be New creates buffered channel C, callback is func(receipt string), and Run drains while C is open in README.md:474. Add one compact canonical block so readers do not need to infer from the Q/A trail.

A: We have the inject message format for the channel and the callback signature. Everything else is not critical.

24. High: current shutdown ownership leaves a race that can break delivery guarantees.
In README.md:508, “inject does not care when sender is closed” conflicts with reliable draining. If caller closes sender while channel is still open, Send will fail and pending items can be lost or stuck in retry semantics that are no longer valid.

A: Again. It is not our problem. We can write the caller should close the sender only after Run() returns. But it does not affect our responsibility. We do not close the channel or the sender. We use them.

25. High: proposing slog.Fatal for runtime errors in this package is dangerous for a library-style component.
In README.md:513, the guidance to call slog.Fatal would terminate the whole process from inside inject logic, which is usually unacceptable for reusable package behavior and makes integration hard to test safely.

A: Lets use this for now. slog.Error("DESIGN ERROR: please look at this fatal error closer")

26. Medium: the API shape is still spread across Q/A notes rather than one authoritative v1 contract.
You have enough pieces in README.md:474, README.md:449, and README.md:499, but implementers still need to infer exact constructor, fields, and run loop behavior from multiple answers.

A: Any constructor that abides by previous conversation is fine. You can write as you like.
