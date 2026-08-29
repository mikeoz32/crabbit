# :nodoc:
module Crabbit::Internal::Wire
  RESPONSE_MASK = 0x8000_u16
  KEY_MASK      = 0x7fff_u16

  enum Command : UInt16
    DeclarePublisher        =  1_u16
    Publish                 =  2_u16
    PublishConfirm          =  3_u16
    PublishError            =  4_u16
    QueryPublisherSequence  =  5_u16
    DeletePublisher         =  6_u16
    Subscribe               =  7_u16
    Deliver                 =  8_u16
    Credit                  =  9_u16
    StoreOffset             = 10_u16
    QueryOffset             = 11_u16
    Unsubscribe             = 12_u16
    CreateStream            = 13_u16
    DeleteStream            = 14_u16
    Metadata                = 15_u16
    MetadataUpdate          = 16_u16
    PeerProperties          = 17_u16
    SaslHandshake           = 18_u16
    SaslAuthenticate        = 19_u16
    Tune                    = 20_u16
    Open                    = 21_u16
    Close                   = 22_u16
    Heartbeat               = 23_u16
    Route                   = 24_u16
    Partitions              = 25_u16
    ConsumerUpdate          = 26_u16
    ExchangeCommandVersions = 27_u16
    StreamStats             = 28_u16
    CreateSuperStream       = 29_u16
    DeleteSuperStream       = 30_u16
    ResolveOffsetSpec       = 31_u16
  end

  SUPPORTED_VERSIONS = {
    Command::DeclarePublisher        => 1_u16..1_u16,
    Command::Publish                 => 1_u16..2_u16,
    Command::PublishConfirm          => 1_u16..1_u16,
    Command::PublishError            => 1_u16..1_u16,
    Command::QueryPublisherSequence  => 1_u16..1_u16,
    Command::DeletePublisher         => 1_u16..1_u16,
    Command::Subscribe               => 1_u16..1_u16,
    Command::Deliver                 => 1_u16..2_u16,
    Command::Credit                  => 1_u16..1_u16,
    Command::StoreOffset             => 1_u16..1_u16,
    Command::QueryOffset             => 1_u16..1_u16,
    Command::Unsubscribe             => 1_u16..1_u16,
    Command::CreateStream            => 1_u16..1_u16,
    Command::DeleteStream            => 1_u16..1_u16,
    Command::Metadata                => 1_u16..1_u16,
    Command::MetadataUpdate          => 1_u16..1_u16,
    Command::PeerProperties          => 1_u16..1_u16,
    Command::SaslHandshake           => 1_u16..1_u16,
    Command::SaslAuthenticate        => 1_u16..1_u16,
    Command::Tune                    => 1_u16..1_u16,
    Command::Open                    => 1_u16..1_u16,
    Command::Close                   => 1_u16..1_u16,
    Command::Heartbeat               => 1_u16..1_u16,
    Command::Route                   => 1_u16..1_u16,
    Command::Partitions              => 1_u16..1_u16,
    Command::ConsumerUpdate          => 1_u16..1_u16,
    Command::ExchangeCommandVersions => 1_u16..1_u16,
    Command::StreamStats             => 1_u16..1_u16,
    Command::CreateSuperStream       => 1_u16..1_u16,
    Command::DeleteSuperStream       => 1_u16..1_u16,
    Command::ResolveOffsetSpec       => 1_u16..1_u16,
  }
end
