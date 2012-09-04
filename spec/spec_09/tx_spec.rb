# encoding: utf-8

# tx_spec.rb

# Assumes that target message broker/server has a user called 'guest' with a password 'guest'
# and that it is running on 'localhost'.

# If this is not the case, please change the 'Bunny.new' call below to include
# the relevant arguments e.g. @b = Bunny.new(:user => 'john', :pass => 'doe', :host => 'foobar')

require "bunny"

describe 'Queue' do

  before(:each) do
    @b = Bunny.new
    @b.start

    @default_exchange = @b.exchange("")
  end

  after(:each) do
    begin
      @b.stop
    rescue Exception
    ensure
      @b = nil
    end
  end

  def message_count(queue, sleep_time = 0.1)
    sleep sleep_time
    queue.message_count
  end

  context "transaction test" do
    let!(:source_q) { @b.queue('test_source') }
    let!(:dest_q) { @b.queue('test_dest') }

    before(:each) do
      source_q.purge.should == :purge_ok
      dest_q.purge.should == :purge_ok
      @b.tx_select
      @default_exchange.publish("Test message", :key => source_q.name)
      @b.tx_commit
    end

    after(:each) do
      source_q.purge.should == :purge_ok
      dest_q.purge.should == :purge_ok
      @b.tx_commit
    end

    it "the transaction should work" do
      message_count(source_q).should == 1
      message_count(dest_q).should == 0

      source_q.pop(:ack => true)
      @default_exchange.publish("Test message", :key => dest_q.name)
      source_q.ack

      @b.tx_commit

      message_count(source_q).should == 0
      message_count(dest_q).should == 1
    end

    it "should rollback the unack-ed message" do
      message_count(source_q).should == 1
      message_count(dest_q).should == 0

      msg = source_q.pop(:ack => true)
      @default_exchange.publish("Test message", :key => dest_q.name)
      source_q.ack

      @b.tx_rollback

      # you have to reject the messages after the commit if you acked them
      # but you need the delivery tag
      source_q.reject(:delivery_tag => msg[:delivery_details][:delivery_tag])
      @b.tx_commit

      message_count(source_q).should == 1
      message_count(dest_q).should == 0
    end
  end
end
