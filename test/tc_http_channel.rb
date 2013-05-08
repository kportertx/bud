require './test_common'
require 'timeout'
require 'socket'               # Get sockets from stdlib

class WebServer
  def self.getServer(port=8888, response = "{\"test_result\":\"1\"}")
    task = Thread.new do
      server = TCPServer.new(port) # Socket to listen on port
      client = server.accept       # Wait for a client to connect
      lines = []
      while line = client.gets and line !~ /^\s*$/
        lines << line.chomp
      end
      puts lines.inspect
      headers = ["HTTP/1.1 200 OK",
                 "Date: Tue, 14 Dec 2010 10:48:45 GMT",
                 "Server: Ruby",
                 "Content-Type: application/json; charset=iso-8859-1",
                 "Content-Length: #{response.length}\r\n\r\n"].join("\r\n")
      client.puts headers          # Send the time to the client
      client.puts response
      client.close                 # Disconnect from the client
    end
    return task
  end
end

class HttpChan
  include Bud

  state do
    httpchannel :testchan, [:@address, :http_type, :id, :val]
  end

  bloom do
    testchan <~ ["127.0.0.1:8888", "GET", 1, "Hello World"]
  end
end

class HttpChanTest < MiniTest::Unit::TestCase
  def test_http_chan
    server = WebServer.getServer
    t = HttpChan.new
    t.tick
    sleep(10)
  end
end
