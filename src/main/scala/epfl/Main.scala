package epfl

import epfl.hello.{GreeterGrpc, HelloReply, HelloRequest}
import io.grpc.{ManagedChannelBuilder, ServerBuilder}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {

  class GreeterImpl extends GreeterGrpc.Greeter {
    override def sayHello(req: HelloRequest) = {
      println(req)
      val reply = HelloReply(message = "Hello " + req.name)
      Future.successful(reply)
    }
  }

  val server = ServerBuilder.forPort(4000).addService(GreeterGrpc.bindService(new GreeterImpl, global)).build
  server.start()

  // no plaintext requires some netty-bundling
  val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 4000).usePlaintext(true).build
  val stub    = GreeterGrpc.blockingStub(channel)

  val request = HelloRequest(name = "World")
  println(stub.sayHello(request))

  sys.addShutdownHook {
    server.shutdown()
  }

}
