# Protocol support

Crabbit follows RabbitMQ server `PROTOCOL.adoc` from the `main` branch. The official Java and Go Stream clients are used only to resolve behavior that is not explicit in the wire specification.

| Area | Support |
| --- | --- |
| Connection | Peer properties, SASL handshake/authentication/challenges, Tune, Open, Close, heartbeat |
| Negotiation | Exchange Command Versions and highest-common-version selection |
| Publishing | Declare/delete publisher, Publish v1/v2, confirms, publish errors, sequence query |
| Consumption | Subscribe/unsubscribe, Deliver v1/v2, credit, consumer updates, CRC validation |
| Offsets | Store, query, and Resolve Offset Specification |
| Metadata | Metadata and stream stats |
| Streams | Create and delete stream |
| Super streams | Create/delete, partitions, route, hash/routing-key producer, partition consumers |
| Payloads | AMQP 1.0 message sections and generic type codec |
| Sub-entry compression | none, gzip, Snappy framed stream, LZ4 frame, Zstandard |

All protocol command keys 1 through 31 are encoded/decoded. Command versions are negotiated at connection startup. Brokers older than 3.11 fall back to the version 1 baseline without attempting the version-exchange command. Publish v2 is selected only when filtering is used; Deliver v1 and v2 are accepted. Other commands currently have protocol version 1 only.

Inbound frames, strings, arrays, maps, chunks, and decompressed sub-entries are bounded before allocation. Deliver chunks validate Osiris magic/type, record and physical-entry counts, declared lengths, optional trailer/bloom lengths, and CRC32.

The wire namespace is internal. Public compatibility is maintained at the `Environment`, `Producer`, `Consumer`, `Message`, and super-stream APIs rather than exposing packet structs.
