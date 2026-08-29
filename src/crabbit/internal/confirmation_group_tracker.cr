module Crabbit::Internal
  # Maps broker confirmation IDs to all publishes represented by that ID.
  # This is one-to-one for ordinary publishes and one-to-many for sub-entries.
  class ConfirmationGroupTracker
    @groups = {} of UInt64 => Array(UInt64)
    @roots = {} of UInt64 => UInt64

    def size : Int32
      @groups.size
    end

    def remember(root_id : UInt64, ids : Array(UInt64)) : Nil
      forget(root_id)
      values = ids.dup
      @groups[root_id] = values
      values.each { |id| @roots[id] = root_id }
    end

    def take(root_id : UInt64) : Array(UInt64)?
      ids = @groups.delete(root_id)
      ids.try &.each { |id| @roots.delete(id) }
      ids
    end

    # Removes a group once none of its members are still pending. This handles
    # terminal local outcomes such as confirmation timeouts with no late frame.
    def finished(id : UInt64, &pending : UInt64 -> Bool) : Nil
      root_id = @roots.delete(id)
      return unless root_id
      ids = @groups[root_id]?
      return unless ids
      return if ids.any? { |member_id| yield member_id }

      @groups.delete(root_id)
      ids.each { |member_id| @roots.delete(member_id) }
    end

    def clear : Nil
      @groups.clear
      @roots.clear
    end

    private def forget(root_id : UInt64) : Nil
      old_ids = @groups.delete(root_id)
      old_ids.try &.each { |id| @roots.delete(id) }
    end
  end

  # Owns pending publishes and their broker confirmation groups behind one
  # mutex. A stale batch can therefore never recreate a group after the
  # corresponding publish has reached a terminal local outcome.
  class ConfirmationTracker(T)
    @mutex = Mutex.new
    @pending = {} of UInt64 => T
    @groups = ConfirmationGroupTracker.new
    @transmitting = Set(UInt64).new

    def add(id : UInt64, value : T) : Bool
      @mutex.synchronize do
        return false if @pending.has_key?(id)

        @pending[id] = value
        true
      end
    end

    def size : Int32
      @mutex.synchronize { @pending.size }
    end

    def values : Array(T)
      @mutex.synchronize { @pending.values.dup }
    end

    # Reserves all still-pending messages for one wire write. Timeout handling
    # defers terminal completion while an ID is reserved, closing the stale-send
    # race without holding the tracker mutex across socket IO.
    def with_pending_singles(ids : Enumerable(UInt64), &send : Array(T) -> Nil) : Bool
      active_ids = [] of UInt64
      values = @mutex.synchronize do
        selected = [] of T
        ids.each do |id|
          if !@transmitting.includes?(id) && (value = @pending[id]?)
            active_ids << id
            selected << value
          end
        end
        return false if selected.empty?

        active_ids.each { |id| @transmitting << id }
        selected
      end
      begin
        yield values
      ensure
        @mutex.synchronize { active_ids.each { |id| @transmitting.delete(id) } }
      end
      true
    end

    # Sub-entry payloads must be encoded from exactly the members that remain
    # pending. Reserve those members before group registration and encoding so
    # a concurrent timeout defers them until after transmission.
    def with_pending_groups(
      groups : Enumerable(Array(UInt64)),
      &send : Array(Tuple(UInt64, Array(T))) -> Nil
    ) : Bool
      transmitting_ids = [] of UInt64
      prepared = @mutex.synchronize do
        selected_groups = [] of Tuple(UInt64, Array(T))
        groups.each do |ids|
          active_ids = [] of UInt64
          values = [] of T
          ids.each do |id|
            if !@transmitting.includes?(id) && (value = @pending[id]?)
              active_ids << id
              values << value
            end
          end
          next if active_ids.empty?

          root_id = active_ids.last
          active_ids.each do |id|
            @transmitting << id
            transmitting_ids << id
          end
          @groups.remember(root_id, active_ids)
          selected_groups << {root_id, values}
        end
        return false if selected_groups.empty?

        selected_groups
      end
      begin
        yield prepared
      ensure
        @mutex.synchronize { transmitting_ids.each { |id| @transmitting.delete(id) } }
      end
      true
    end

    # Avoid allocating a one-element fallback array for every ordinary broker
    # confirmation while retaining the array-backed sub-entry path.
    def each_group(root_id : UInt64, & : UInt64 -> Nil) : Nil
      ids = @mutex.synchronize { @groups.take(root_id) }
      if ids
        ids.each { |id| yield id }
      else
        yield root_id
      end
    end

    def finish(id : UInt64, defer_if_transmitting : Bool = false) : T?
      @mutex.synchronize do
        return nil if defer_if_transmitting && @transmitting.includes?(id)

        value = @pending.delete(id)
        @transmitting.delete(id)
        @groups.finished(id) { |member_id| @pending.has_key?(member_id) }
        value
      end
    end

    def clear_groups : Nil
      @mutex.synchronize { @groups.clear }
    end

    def take_all : Array(T)
      @mutex.synchronize do
        values = @pending.values.dup
        @pending.clear
        @groups.clear
        @transmitting.clear
        values
      end
    end

    # Internal diagnostic used by focused invariant tests.
    def group_count : Int32
      @mutex.synchronize { @groups.size }
    end
  end
end
