# Java reference examples in Crystal

These programs mirror every documentation snippet in RabbitMQ Stream Java
client **v1.11.0** (`d9bdcfec9bf3640cc4df6ef5eff82ce3b1649540`). The same
six documentation source classes and tagged snippets are present at Java client
main commit `3950fc56690706535a3d2e996341ce3cfc007979`.

Set `CRABBIT_STREAM_URI` when the broker does not use the default
`guest:guest@localhost:5552/%2f` credentials:

```bash
CRABBIT_STREAM_URI=rabbitmq-stream://crabbit:crabbit@localhost:5552/%2f \
  crystal run examples/java_reference/sample_application.cr
```

`CRABBIT_EXAMPLE_MESSAGE_COUNT` changes the sample application's default 10,000
messages. `CRABBIT_EXAMPLE_CA_CERT` enables the custom-CA TLS configuration
snippet in `environment_usage.cr`.

## Coverage

| Java documentation source | Crystal program | Covered tags |
| --- | --- | --- |
| `SampleApplication.java` | `sample_application.cr` | `sample-imports`, `sample-environment`, `sample-publisher`, `sample-consumer`, `sample-environment-close` |
| `EnvironmentUsage.java` | `environment_usage.cr` | environment creation, URI/URI list, TLS custom CA/trust-all, load-balancer address resolution, create/delete stream, byte/time retention, OAuth 2 token lifecycle |
| `ProducerUsage.java` | `producer_usage.cr` | creation, publish callback, complex AMQP message, named producer, explicit publishing ID, last ID query, sub-entry batching, Zstandard compression |
| `ConsumerUsage.java` | `consumer_usage.cr` | creation, default/configured auto tracking, named consumer, default/configured manual tracking, subscription offset callback, flow control, Single Active Consumer/update callback |
| `FilteringUsage.java` | `filtering_usage.cr` | producer extraction, consumer filter plus mandatory post-filter, match-unfiltered |
| `SuperStreamUsage.java` | `super_stream_usage.cr` | partition-count/binding-key creation, hash/custom-hash/key/custom routing, consumer, SAC, broker/manual and external offset tracking |

This covers 47 of the 49 snippets as executable Crystal equivalents. The two
remaining snippets configure Java-specific infrastructure, not Stream wire
commands:

- `native-epoll`: Netty transport selection is not applicable to Crystal's
  runtime-managed non-blocking socket scheduler.
- `micrometer-observation`: Crabbit does not depend on the Java Micrometer
  observation API.
The examples are compile-checked by every unit CI lane. The pinned and latest
Docker integration lanes execute all six programs; unreachable cluster/TLS
configurations in `environment_usage.cr` are constructed but intentionally not
connected.
