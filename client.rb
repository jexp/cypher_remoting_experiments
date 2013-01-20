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

def error_check(rc)
  if ZMQ::Util.resultcode_ok?(rc)
    false
  else
    STDERR.puts "Operation failed, errno [#{ZMQ::Util.errno}] description [#{ZMQ::Util.error_string}]"
    caller(1).each { |callstack| STDERR.puts(callstack) }
    true
  end
end

ctx = ZMQ::Context.create(1)
STDERR.puts "Failed to create a Context" unless ctx

req_sock = ctx.socket(ZMQ::REQ)
rc = req_sock.connect('tcp://localhost:5555')
STDERR.puts "Failed to connect REQ socket" unless ZMQ::Util.resultcode_ok?(rc)

query ='start n=node({id}) return n'
msg = { :query => query, :stats => true, :params => {:id => 0}}
rc = req_sock.send_string(MessagePack.pack(msg))
break if error_check(rc)

rep = ''
begin
  rc = req_sock.recv_string(rep)
  break if error_check(rc)
  puts "Received reply '#{MessagePack.unpack(rep).inspect}'"
end while req_sock.more_parts?

error_check(req_sock.close)

ctx.terminate

# A successful run looks like:

#$ ruby 003_req_rep.rb 
#Received request 'Marco...'
#Received reply 'Polo!'
#Received request 'Marco...'
#Received reply 'Polo!'
#Operation failed, errno [156384765] description [Context was terminated]
#003_req_rep.rb:41:in `__script__'
#Closed REP socket; terminating thread...