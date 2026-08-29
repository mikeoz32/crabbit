require "log"
require "socket"
require "openssl"
require "digest/crc32"
require "http/client"
require "json"
require "uri/params"

require "./crabbit/version"
require "./crabbit/errors"
require "./crabbit/response_code"
require "./crabbit/types"
require "./crabbit/configuration"
require "./crabbit/oauth2"
require "./crabbit/amqp/value"
require "./crabbit/message"
require "./crabbit/amqp/codec"
require "./crabbit/amqp/message_codec"
require "./crabbit/compression"
require "./crabbit/options"

require "./crabbit/internal/wire/constants"
require "./crabbit/internal/wire/reader"
require "./crabbit/internal/wire/writer"
require "./crabbit/internal/wire/frame"
require "./crabbit/internal/wire/commands"
require "./crabbit/internal/sub_entry"
require "./crabbit/internal/delivery_offset_tracker"
require "./crabbit/internal/confirmation_group_tracker"
require "./crabbit/internal/connection"
require "./crabbit/internal/deliver_parser"
require "./crabbit/internal/client"
require "./crabbit/environment"
require "./crabbit/producer"
require "./crabbit/consumer"
require "./crabbit/super_stream"

# Native asynchronous client for the RabbitMQ Stream protocol.
#
# `Crabbit` exposes connection management through `Environment`, publishing
# through `Producer`, consumption through `Consumer`, and complete AMQP 1.0
# message encoding through `Message` and `AMQP`.
#
# Applications only need the top-level require:
#
# ```
# require "crabbit"
#
# environment = Crabbit::Environment.connect
# producer = environment.producer("events")
# producer.publish("hello").await
# ```
#
# Resources own sockets and background fibers. Close producers and consumers
# when they are no longer needed, or call `Environment#close` to close every
# resource created by the environment.
module Crabbit
  Log = ::Log.for("crabbit")
end
