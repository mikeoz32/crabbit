require "./spec_helper"

describe Crabbit::Murmur3 do
  it "matches the RabbitMQ Java client's unsigned Murmur3 vectors" do
    {
      "hello" => 1_321_743_225_u32,
      "brave" => 3_825_276_426_u32,
      "new"   => 2_970_740_106_u32,
      "world" => 2_453_398_188_u32,
    }.each do |value, expected|
      Crabbit::Murmur3.hash32(value).should eq(expected)
    end
  end

  it "supports a caller-provided seed" do
    Crabbit::Murmur3.hash32("hello", 42_u32).should eq(3_806_057_185_u32)
  end
end
