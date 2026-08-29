module Crabbit::Internal
  # Tracks acknowledgements in broker delivery order. Stream offsets are not
  # necessarily adjacent when server-side filtering skips chunks.
  class DeliveryOffsetTracker
    getter last : UInt64?

    @order = [] of UInt64
    @head = 0
    @registered = Set(UInt64).new
    @processed = Set(UInt64).new

    def register(offset : UInt64) : Nil
      if last = @last
        return if offset <= last
      end
      return if @registered.includes?(offset)

      @registered << offset
      @order << offset
    end

    # Returns the number of deliveries that became consecutively processed in
    # registration order after this acknowledgement.
    def processed(offset : UInt64) : Int32
      return 0 unless @registered.includes?(offset)

      @processed << offset
      advanced = 0
      while expected = @order[@head]?
        break unless @processed.delete(expected)

        @registered.delete(expected)
        @last = expected
        @head += 1
        advanced += 1
      end
      compact_order
      advanced
    end

    private def compact_order : Nil
      return unless @head >= 1_024 && @head * 2 >= @order.size

      @order = @order[@head, @order.size - @head]
      @head = 0
    end
  end
end
