module Crabbit
  # Base class for all Crabbit-specific failures.
  class Error < Exception
  end

  # Raised when local options or arguments form an invalid configuration.
  class ConfigurationError < Error
  end

  # Raised when a peer sends malformed or unexpected protocol data.
  class ProtocolError < Error
  end

  # Raised when an encoded frame exceeds the negotiated maximum size.
  class FrameTooLargeError < ProtocolError
    # Returns the attempted frame size in bytes.
    getter size : UInt32
    # Returns the negotiated frame-size limit in bytes.
    getter limit : UInt32

    # Creates a frame-size error.
    def initialize(@size : UInt32, @limit : UInt32)
      super("frame size #{size} exceeds negotiated limit #{limit}")
    end
  end

  # Raised when a TCP, TLS, heartbeat, or connection-level operation fails.
  class ConnectionError < Error
  end

  # Raised when an operation needs a connection that is already closed.
  class ConnectionClosedError < ConnectionError
  end

  # Raised when a configured request, enqueue, or confirmation deadline expires.
  class TimeoutError < Error
  end

  # Raised when SASL authentication or re-authentication fails.
  class AuthenticationError < ConnectionError
  end

  # Raised when OAuth 2 token retrieval or validation fails.
  class OAuth2Error < AuthenticationError
  end

  # Raised when RabbitMQ returns a non-success `ResponseCode`.
  class BrokerError < Error
    # Returns the broker response code.
    getter code : ResponseCode

    # Creates an error for *code* with an optional custom *message*.
    def initialize(@code : ResponseCode, message : String? = nil)
      super(message || "broker returned #{code}")
    end
  end

  # Raised when an operation targets a producer, consumer, or environment that
  # the application permanently closed.
  class ResourceClosedError < Error
  end

  # Raised when a compression codec fails or produces an invalid output size.
  class CompressionError < Error
  end

  # Raised when AMQP or Stream payload encoding/decoding fails validation.
  class CodecError < Error
  end
end
