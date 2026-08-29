# Crabbit guides

The [generated API reference](https://mikeoz32.github.io/crabbit/) is the source of truth for public types, signatures, defaults, ownership, and exceptions. These guides explain how those pieces fit together in an application.

| Guide | Covers |
| --- | --- |
| [Getting started](getting-started.md) | Installation, broker setup, first producer and consumer, shutdown |
| [Publishing](publishing.md) | Payload forms, confirmations, batching, compression, filtering, deduplication |
| [Consuming](consuming.md) | Push and pull APIs, credit, processing, offsets, filtering, SAC |
| [Configuration](configuration.md) | URIs, endpoints, TLS, load balancers, SASL, timeouts, execution contexts |
| [Super streams](super-streams.md) | Topology, routing, partition producers and consumers |
| [OAuth 2](oauth2.md) | Client credentials, custom CAs, refresh, live re-authentication, custom providers |
| [AMQP codec](amqp-codec.md) | Message sections, values, direct-to-IO encoding, zero-copy decoding |
| [Operations](operations.md) | Recovery, delivery guarantees, error handling, logging, graceful shutdown |
| [Protocol support](protocol-support.md) | Implemented commands, versions, bounds, and compatibility scope |

## Documentation commands

Generate the same site published by GitHub Pages:

```bash
crystal docs src/crabbit.cr \
  --output=site \
  --project-name=Crabbit \
  --canonical-base-url=https://mikeoz32.github.io/crabbit/ \
  --base-path=/crabbit
```

Open `site/index.html` locally. The generated `site/index.json` contains the complete machine-readable API model.

Check that every public Crabbit type and explicitly declared API method has a doc comment:

```bash
crystal run scripts/check_docs.cr -- site/index.json
```
