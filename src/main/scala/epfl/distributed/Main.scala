package epfl.distributed

import epfl.distributed.hello.{GreeterGrpc, HelloReply, HelloRequest}
import io.grpc.{ManagedChannelBuilder, ServerBuilder}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

//import Vec._

object Main extends App {

  val v1 = Vec(1, 2, 3)
  val v2 = Vec(1, 2, 3)

  v1 + v2
  v1 dot v2
  v1 * 2
  v1.norm

  class GreeterImpl extends GreeterGrpc.Greeter {
    override def sayHello(req: HelloRequest) = {
      println(req)
      val reply = HelloReply(message = "Hello " + req.name)
      Future.successful(reply)
    }
  }

  val server = ServerBuilder.forPort(config.port).addService(GreeterGrpc.bindService(new GreeterImpl, global)).build
  server.start()

  // no plaintext requires some netty-bundling
  val channel = ManagedChannelBuilder.forAddress("127.0.0.1", config.port).usePlaintext(true).build
  val stub    = GreeterGrpc.blockingStub(channel)

  val request = HelloRequest(name = "World")
  println(stub.sayHello(request))

  sys.addShutdownHook {
    server.shutdown()
  }

}
