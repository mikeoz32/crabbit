# AMQP 1.0 messages and codec

RabbitMQ Streams stores opaque message bytes. Crabbit uses the AMQP 1.0 message format used by the official Stream clients, independently from the AMQP 1.0 transport protocol.

## Message sections

`Message` represents:

1. Header
2. Delivery annotations
3. Message annotations
4. Properties
5. Application properties
6. Data, Sequence, or Value body
7. Footer
8. Unknown described sections preserved in `extra_sections`

```crystal
message = Crabbit::Message.new(
  "invoice-created",
  header: Crabbit::Header.new(durable: true),
  properties: Crabbit::Properties.new(
    message_id: "invoice-123",
    content_type: "application/json",
    creation_time: Time.utc,
    group_id: "customer-42",
  ),
  application_properties: {
    "region"  => Crabbit::AMQP::Value.wrap("eu"),
    "attempt" => Crabbit::AMQP::Value.wrap(1_u32),
  },
)
```

AMQP symbols such as `content_type` and `content_encoding` must contain ASCII only. Strings use UTF-8.

## Body families

The normal constructor and `.data` create Data bodies:

```crystal
single = Crabbit::Message.new(payload)
multiple = Crabbit::Message.data([part_a, part_b])
```

Data parts are distinct AMQP sections. `Message#body` returns the only part directly or joins multiple parts into a new slice.

Sequence bodies carry one or more AMQP lists:

```crystal
message = Crabbit::Message.sequence([
  Crabbit::AMQP::Value.wrap("north"),
  Crabbit::AMQP::Value.wrap(42_i64),
])
```

Value bodies carry one arbitrary AMQP value:

```crystal
message = Crabbit::Message.value(
  Crabbit::AMQP::Value.map({"status" => "ready"})
)
```

Only one body family may appear in a valid message. The decoder raises `CodecError` if Data, Sequence, and Value sections are mixed.

## Direct-to-IO encoding

The allocating API returns owned bytes:

```crystal
encoded = message.to_amqp
```

The streaming overload writes directly to any `IO`:

```crystal
buffer = IO::Memory.new
message.to_amqp(buffer)
```

This avoids an intermediate encoded `Bytes` allocation and is the preferred path when the destination is already an `IO`. `AMQP::Encoder.encode(value, io)` offers the same behavior for standalone AMQP values.

## Decoding and ownership

Safe decoding copies binary values:

```crystal
message = Crabbit::Message.from_amqp(encoded)
```

Zero-copy decoding references the input for Data sections and AMQP binary values:

```crystal
message = Crabbit::Message.from_amqp(encoded, zero_copy: true)
```

With `zero_copy: true`, keep `encoded` alive and immutable for as long as the decoded message or any derived binary slice is used. Consumer deliveries use zero-copy decoding against `Delivery#raw`, which owns the encoded storage.

## Generic AMQP values

`AMQP::Value.wrap` maps Crystal primitives to canonical AMQP kinds:

| Crystal | AMQP kind |
| --- | --- |
| `Nil` | Null |
| `Bool` | Boolean |
| `UInt8`, `UInt16`, `UInt32`, `UInt64` | UByte, UShort, UInt, ULong |
| `Int8`, `Int16`, `Int32`, `Int64` | Byte, Short, Int, Long |
| `Float32`, `Float64` | Float, Double |
| `Char` | Char |
| `Time` | Timestamp |
| `AMQP::UUID` | UUID |
| `Bytes` | Binary |
| `String` | String |

Use explicit constructors for other kinds:

```crystal
symbol = Crabbit::AMQP::Value.symbol("application/json")
list = Crabbit::AMQP::Value.list([1_u32, "two", true])
map = Crabbit::AMQP::Value.map({"key" => "value"})
array = Crabbit::AMQP::Value.array([1_u32, 2_u32, 3_u32])
described = Crabbit::AMQP::Value.described(0x123_u64, "payload")
```

AMQP arrays must be non-empty and homogeneous at the AMQP constructor level. Maps preserve arbitrary AMQP key types; application-properties keys are specifically strings.

Decimal32, Decimal64, and Decimal128 are represented by opaque 4-, 8-, or 16-byte `AMQP::Decimal` payloads so the exact IEEE interchange representation round-trips without precision loss.

## Standalone value codec

```crystal
value = Crabbit::AMQP::Value.list(["event", 7_u64])
encoded = Crabbit::AMQP::Encoder.encode(value)

decoder = Crabbit::AMQP::Decoder.new(encoded)
decoded = decoder.read
raise Crabbit::CodecError.new("trailing bytes") unless decoder.eof?
```

The decoder validates type codes, lengths, UTF-8, ASCII symbols, container sizes, even map element counts, homogeneous arrays, and truncation before reading or allocating. Container declarations are capped at one million elements.

## Unknown described sections

The message decoder preserves unrecognized described sections as generic AMQP values in `Message#extra_sections`. Re-encoding writes them after the standard body and before the footer. This permits forward-compatible round trips without making unknown sections part of Crabbit's typed message model.
