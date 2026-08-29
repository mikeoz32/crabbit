require "../spec_helper"

describe Crabbit::Internal::DeliveryOffsetTracker do
  it "advances in delivery order when filtered offsets are not adjacent" do
    tracker = Crabbit::Internal::DeliveryOffsetTracker.new
    [0_u64, 3_u64, 5_u64].each { |offset| tracker.register(offset) }

    tracker.processed(3_u64).should eq 0
    tracker.last.should be_nil
    tracker.processed(0_u64).should eq 2
    tracker.last.should eq 3_u64
    tracker.processed(5_u64).should eq 1
    tracker.last.should eq 5_u64
  end

  it "does not re-register pending or completed offsets during recovery" do
    tracker = Crabbit::Internal::DeliveryOffsetTracker.new
    [0_u64, 3_u64].each { |offset| tracker.register(offset) }
    tracker.processed(0_u64).should eq 1

    [3_u64, 5_u64].each { |offset| tracker.register(offset) }
    tracker.processed(3_u64).should eq 1
    tracker.processed(5_u64).should eq 1
    tracker.last.should eq 5_u64
  end
end

describe Crabbit::Internal::ConfirmationGroupTracker do
  it "removes ordinary confirmation groups after a terminal timeout" do
    tracker = Crabbit::Internal::ConfirmationGroupTracker.new
    tracker.remember(7_u64, [7_u64])

    tracker.finished(7_u64) { false }

    tracker.size.should eq 0
  end

  it "keeps a sub-entry group until every member has finished" do
    tracker = Crabbit::Internal::ConfirmationGroupTracker.new
    pending = Set{1_u64, 2_u64}
    tracker.remember(2_u64, [1_u64, 2_u64])

    pending.delete(1_u64)
    tracker.finished(1_u64) { |id| pending.includes?(id) }
    tracker.size.should eq 1
    pending.delete(2_u64)
    tracker.finished(2_u64) { |id| pending.includes?(id) }
    tracker.size.should eq 0
  end
end

describe Crabbit::Internal::ConfirmationTracker do
  it "does not replace a pending explicit publishing ID" do
    tracker = Crabbit::Internal::ConfirmationTracker(String).new

    tracker.add(7_u64, "first").should be_true
    tracker.add(7_u64, "second").should be_false
    tracker.finish(7_u64).should eq("first")
  end

  it "prepares ordinary publishes without allocating a confirmation group" do
    tracker = Crabbit::Internal::ConfirmationTracker(String).new
    tracker.add(7_u64, "publish")

    marked = nil
    tracker.with_pending_singles([7_u64]) { |values| marked = values.first }.should be_true

    marked.should eq "publish"
    tracker.group_count.should eq 0
    yielded = [] of UInt64
    tracker.each_group(7_u64) { |id| yielded << id }
    yielded.should eq [7_u64]
  end

  it "defers a timeout until its wire write completes" do
    tracker = Crabbit::Internal::ConfirmationTracker(String).new
    tracker.add(7_u64, "publish")
    entered = Channel(Nil).new
    release = Channel(Nil).new
    wire_done = Channel(Nil).new
    finished = Channel(String?).new

    spawn do
      tracker.with_pending_singles([7_u64]) do |values|
        values.should eq ["publish"]
        entered.send(nil)
        release.receive
      end
      wire_done.send(nil)
    end
    entered.receive
    spawn { finished.send(tracker.finish(7_u64, defer_if_transmitting: true)) }
    finished.receive.should be_nil

    release.send(nil)
    wire_done.receive
    tracker.finish(7_u64, defer_if_transmitting: true).should eq "publish"
  end

  it "rebuilds sub-entry groups from only the members still pending" do
    tracker = Crabbit::Internal::ConfirmationTracker(String).new
    tracker.add(2_u64, "pending")

    prepared = nil
    tracker.with_pending_groups([[1_u64, 2_u64]]) { |groups| prepared = groups }.should be_true

    prepared.should eq [{2_u64, ["pending"]}]
    yielded = [] of UInt64
    tracker.each_group(2_u64) { |id| yielded << id }
    yielded.should eq [2_u64]
  end

  it "removes a prepared group when timeout wins after preparation" do
    tracker = Crabbit::Internal::ConfirmationTracker(String).new
    tracker.add(7_u64, "publish")
    tracker.with_pending_groups([[7_u64]]) { }.should be_true

    tracker.finish(7_u64).should eq "publish"

    tracker.group_count.should eq 0
  end

  it "does not recreate a group when a stale batch arrives after timeout" do
    tracker = Crabbit::Internal::ConfirmationTracker(String).new
    tracker.add(7_u64, "publish")
    tracker.finish(7_u64).should eq "publish"

    prepared = tracker.with_pending_groups([[7_u64]]) { }

    prepared.should be_false
    tracker.group_count.should eq 0
  end

  it "tracks only pending members when a stale sub-entry is prepared" do
    tracker = Crabbit::Internal::ConfirmationTracker(String).new
    tracker.add(2_u64, "pending")

    prepared = tracker.with_pending_groups([[1_u64, 2_u64]]) { }

    prepared.should be_true
    yielded = [] of UInt64
    tracker.each_group(2_u64) { |id| yielded << id }
    yielded.should eq [2_u64]
    tracker.group_count.should eq 0
  end
end
