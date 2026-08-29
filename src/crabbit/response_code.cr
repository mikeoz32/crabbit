module Crabbit
  # RabbitMQ Stream protocol response status.
  #
  # `Ok` is the sole success value. Other values can be inspected on
  # `BrokerError#code` or converted to an exception with `#raise_unless_ok!`.
  enum ResponseCode : UInt16
    Ok                            = 0x01_u16
    StreamDoesNotExist            = 0x02_u16
    SubscriptionIdAlreadyExists   = 0x03_u16
    SubscriptionIdDoesNotExist    = 0x04_u16
    StreamAlreadyExists           = 0x05_u16
    StreamNotAvailable            = 0x06_u16
    SaslMechanismNotSupported     = 0x07_u16
    AuthenticationFailure         = 0x08_u16
    SaslError                     = 0x09_u16
    SaslChallenge                 = 0x0a_u16
    AuthenticationFailureLoopback = 0x0b_u16
    VirtualHostAccessFailure      = 0x0c_u16
    UnknownFrame                  = 0x0d_u16
    FrameTooLarge                 = 0x0e_u16
    InternalError                 = 0x0f_u16
    AccessRefused                 = 0x10_u16
    PreconditionFailed            = 0x11_u16
    PublisherDoesNotExist         = 0x12_u16
    NoOffset                      = 0x13_u16
    SaslCannotChangeMechanism     = 0x14_u16
    SaslCannotChangeUsername      = 0x15_u16

    # Returns whether this code represents success.
    def ok? : Bool
      self == Ok
    end

    # Raises `BrokerError` unless this code is `Ok`.
    #
    # Optional *context* is prepended to the generated error message.
    def raise_unless_ok!(context : String? = nil) : Nil
      return if ok?

      detail = context ? "#{context}: broker returned #{self}" : nil
      raise BrokerError.new(self, detail)
    end
  end
end
