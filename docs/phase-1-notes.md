# Phase 1 — learning notes

What Phase 1 taught, as seven questions and answers. The commands and code behind each answer are in [phase-1-steps.md](phase-1-steps.md).

## 1. What does `flush()` do, and what happens without it?

`produce()` does not send anything. It puts the message in a buffer inside the program and returns at once. The client sends the buffer in batches, one batch per partition, which is what lets a producer send hundreds of thousands of messages per second.

`flush()` sends whatever is still in the buffer and waits until Kafka has confirmed every message. Without it, a short program can finish and exit while messages are still in the buffer, and they are never sent.

The delivery callback is how the producer learns the outcome: for each message it reports the partition and offset it landed at, or the error.

## 2. What is a committed offset?

An offset is a message's position in a partition: 0, 1, 2 and so on. It never changes and is never reused.

A committed offset is the position a consumer group has saved, per partition: "the next message this group will read". Kafka stores it, not the consumer, so it survives the consumer stopping. A consumer that restarts with the same `group.id` carries on from the committed offset instead of starting over. In step 2 the restarted consumer printed nothing, because the group had already read up to offset 6.

**Lag** is the newest offset minus the committed offset: how far behind the group is. A lag of 0 means the group has read everything.

`auto.offset.reset: earliest` only matters the first time, when a group has no committed offset yet.

## 3. Why is `events.raw` keyed by `product_id`?

Kafka hashes the key to choose the partition, so every message with the same key lands in the same partition. Order is guaranteed only within a partition. Keying by `product_id` therefore keeps all events for one product together and in the order they happened, which the feature consumer needs when it counts sales per product per hour.

The spread is not even. In step 3, 8 keys on 6 partitions left partition 2 empty and put three keys on partition 4. With thousands of products this evens out, but one very popular product still makes its partition busier than the rest (a hot key).

## 4. What happened when Reader B joined and then left?

Reader A and Reader B were two copies of the same program with the same `group.id`, so Kafka treated them as one team. Kafka's rule is that each partition goes to exactly one member of a team.

- A alone owned all 6 partitions.
- When B joined, Kafka took the partitions back from A (revoked) and shared them out (assigned): A got 3, 4 and 5; B got 0, 1 and 2.
- New messages went to whoever owned their partition. No message was read twice and none was skipped.
- When B stopped with Ctrl+C, it told Kafka it was leaving, and Kafka gave all 6 partitions back to A straight away.

This is a **rebalance**, and it is how a consumer scales: run more copies with the same group name. More copies than partitions does not help; the extra ones get nothing. Copies with *different* group names do not share; each group reads everything.

## 5. What does a compacted topic keep?

A normal topic deletes messages by age. A compacted topic keeps the **latest message for each key** and eventually discards older messages with the same key. `catalog.products` is compacted and keyed by `product_id`, so it works as a table of the current details of every product: reading it from the beginning rebuilds the whole catalog, and old products are never lost just because they are old.

## 6. What did the Avro serializer reject and accept?

It **rejected**:

- `price = "twelve"`: "could not convert string to float";
- `product_id = "42"`: "an integer is required on field product_id";
- `event_type = "click"`: not one of the four allowed values.

It **accepted** `price = "12.50"`, a number written as text, and quietly stored 12.5. The serializer tries to convert text to a decimal number before rejecting it. What lands in Kafka has the right type, but a program sending text by mistake gets no warning. The Phase 2 replayer reads a CSV, which is all text, so it must convert every column to its proper type itself.

It also accepted `brand` missing, as the schema allows (`["null", "string"]`).

Size: the demo event was 72 bytes as Avro and 241 bytes as JSON, because Avro does not repeat field names in every message. Each Avro message starts with a marker byte and the 4-byte schema ID, which is how a consumer knows which schema to fetch. The consumer is never given a schema file.

## 7. Which schema change is allowed under BACKWARD?

BACKWARD means a consumer using the new schema must still be able to read messages written with the old one.

| Change | Allowed | Why |
|---|---|---|
| Add `discount` with a default | Yes | Old messages have no discount; the default fills in. |
| Add `discount` without a default | No | Old messages have no discount and there is nothing to fill in. |
| Rename `product_id` to `item_id` | No | The registry sees "remove `product_id`, add `item_id` with no default", so old messages cannot supply `item_id`. |

`test_compatibility` checks a change without registering it, so it is safe to try.

## Other things worth remembering

- **Save before running.** Five times this phase and last, a command ran against a file whose edits were still only in the editor. The signs: no output at all from a program, `collected 1 item` when more tests were expected, or a lint error that was already fixed. VS Code's File → Auto Save prevents it.
- **A module name must match the file name exactly.** `keyes_producer.py` cannot be run as `streamml.basics.keyed_producer`.
- **Run `make lint` before committing.** `ruff` checks more than syntax: it rejected `timezone.utc` (use `UTC`) and `except Exception` (name the errors you expect).
- **`timestamp-millis` keeps milliseconds.** Anything finer is dropped.
- **`main` changes only through a pull request.** `git push` on `develop` updates `develop` on GitHub; `main` shows the last merged phase until the next pull request is merged.
- **A CI run can wait because of GitHub, not the code.** On 2026-10-05 a run sat queued during a GitHub Actions outage, then passed once Actions recovered.
