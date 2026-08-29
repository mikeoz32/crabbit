module Crabbit
  class Error < Exception
  end

  class ConfigurationError < Error
  end

  class ProtocolError < Error
  end

  class FrameTooLargeError < ProtocolError
    getter size : UInt32
    getter limit : UInt32

    def initialize(@size : UInt32, @limit : UInt32)
      super("frame size #{size} exceeds negotiated limit #{limit}")
    end
  end

  class ConnectionError < Error
  end

  class ConnectionClosedError < ConnectionError
  end

  class TimeoutError < Error
  end

  class AuthenticationError < ConnectionError
  end

  class OAuth2Error < AuthenticationError
  end

  class BrokerError < Error
    getter code : ResponseCode

    def initialize(@code : ResponseCode, message : String? = nil)
      super(message || "broker returned #{code}")
    end
  end

  class ResourceClosedError < Error
  end

  class CompressionError < Error
  end

  class CodecError < Error
  end
end
