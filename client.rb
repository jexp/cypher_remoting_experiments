require 'rubygems'
require 'ffi-rzmq'
require 'msgpack'

Thread.abort_on_exception = true

# REQ and REP sockets work together to establish a synchronous bidirectional flow of data.
# You can think of REQ and REP much like you'd think of a protocol like HTTP, you send a request,
# and you get a response. In between the request and response the thread is blocked.
# 
# REQ sockets are load balanced among all clients, exactly like PUSH sockets. REP responses are
# correctly routed back to the originating REQ socket.
#
# To start, we're going to build a simple rep/req message system that looks like this:
#
#                          req_sock
#                             |
#                          rep_sock
#

class Client
def initialize()
@ctx = ZMQ::Context.create(1)
STDERR.puts "Failed to create a Context" unless @ctx

@req_sock = @ctx.socket(ZMQ::REQ)
rc = @req_sock.connect('tcp://localhost:5555')
STDERR.puts "Failed to connect REQ socket" unless ZMQ::Util.resultcode_ok?(rc)
end
def query(query, opts={})
   msg = { :query => query, :stats => false, :params => {:id => 0}}.merge(opts)
#   puts msg.inspect
   rc = @req_sock.send_string(MessagePack.pack(msg))
   break if error_check(rc)

   rep = ''
   res = {}
   begin
     rc = @req_sock.recv_string(rep)
     break if error_check(rc)
     res = MessagePack.unpack(rep) if !@req_sock.more_parts?
# puts "Received reply '#{res.inspect}'"
   end while @req_sock.more_parts?
   res
end

def error_check(rc)
  if ZMQ::Util.resultcode_ok?(rc)
    false
  else
    STDERR.puts "Operation failed, errno [#{ZMQ::Util.errno}] description [#{ZMQ::Util.error_string}]"
    caller(1).each { |callstack| STDERR.puts(callstack) }
    true
  end
end
def close
    error_check(@req_sock.close)

    @ctx.terminate
end
end

=begin
start = Time.now.to_i
client = Client.new
10.times do
res = client.query(nil,{:tx=>:begin,:stats=>true})
1000.times { |i| client.query('create n={name:{name}}',{:tx_id=>res['tx_id'],:params => {:name => "test#{i}"}}) }
res = client.query(nil,{:tx=>:commit,:tx_id=>res['tx_id']})
end
client.close
stop = Time.now.to_i
puts "With large tx, took #{stop-start} s."
=end

start = Time.now.to_i
client = Client.new
10.times do
1000.times { |i| client.query('create n={name:{name}}',{:params => {:name => "test#{i}"}}) }
end
client.close
stop = Time.now.to_i
puts "Without tx, took #{stop-start} s."
