import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.io.*;



/*
  Server class
  1. Receives input from Client
  2. Creates the main thread for Server and WorkerNode interaction
*/

public class Node {
    public static NodeHandler handler;
    public static ClientNodeInterface.Processor processor;
    static String ip, port;
    public static void main(String [] args) {
        System.out.println(args.length);
        System.out.println(args[0]+" "+args[1]);
        ip = args[0];
        port = args[1];
        try {
            handler = new NodeHandler();
            processor = new ClientNodeInterface.Processor(handler);

            handler.connectToSuperNode(ip, port);

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor);
                }
            };
            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(ClientNodeInterface.Processor processor) {
        try {
                // TServerTransport serverTransport = new TServerSocket(9092);
                TServerTransport serverTransport = new TServerSocket(Integer.valueOf(port));
                TTransportFactory factory = new TFramedTransport.Factory();
                TServer.Args args = new TServer.Args(serverTransport);
                args.processor(processor);
                args.transportFactory(factory);
                TServer server = new TSimpleServer(args);
                //TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor).transportFactory(factory));
                server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
