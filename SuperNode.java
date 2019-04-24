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

public class SuperNode {
    public static SuperNodeHandler handler;
    public static SuperNodeInterface.Processor processor;


    public static void main(String [] args) {
        try {
            handler = new SuperNodeHandler();
            processor = new SuperNodeInterface.Processor(handler);

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

    public static void simple(SuperNodeInterface.Processor processor) {
            int portNumber = 0;
            try{
              BufferedReader br = new BufferedReader(new FileReader("./config.txt"));
              int count = 0;
              String st;
              while ((st = br.readLine()) != null){
                if (count == 1){
                    String[] strArr = st.split(" ");
                    try{
                        portNumber = Integer.parseInt(strArr[1]);
                    }
                    catch(Exception e){
                      System.out.println("Port Number should be a valid Integer");
                      return;
                    }
                    break;
                }
                ++count;
              }
            }
            catch(Exception e){}

        try {
                TServerTransport serverTransport = new TServerSocket(portNumber);
                TTransportFactory factory = new TFramedTransport.Factory();
                TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor).transportFactory(factory));
                server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
