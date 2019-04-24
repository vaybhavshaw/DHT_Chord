import org.apache.thrift.TException;
import java.util.*;
import java.util.concurrent.*;
import java.lang.*;
import java.io.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.lang.*;

/*
  SuperNodeHandler Class:
  1. returns an arbitrary node to the Client (for connection between Client -> Node)
  2. Helps new node to Join the DHT
  3. Maintains a mutex to disallow multiple Nodes to join at the same time
*/

public class SuperNodeHandler implements SuperNodeInterface.Iface{

        volatile static int mutex = 0; // helps maintain synchronization in joining
        ArrayList<NodeStruct> arrListNodeDetails = new ArrayList<>();
        HashSet<Integer> setIds = new HashSet<>();
        int portNumber = 0;
        int bit = 0;
        int bitSpan = 0;

        @Override
        public boolean ping() throws TException {
			       return true;
		    }

        @Override
          public String getNode() throws TException {
               if(mutex == 1) {
                  return "SuperNode busy: Try Again Later";
               }
               int s = arrListNodeDetails.size();
               System.out.println("#nodes registerd with SuperNode :"+arrListNodeDetails.size());
               if(s == 0) return "No node registered with SuperNode";
               if(s!=0){
                  Random rand = new Random();
                  int n = rand.nextInt(s);
                  return arrListNodeDetails.get(n).ipPort;
               }
              return "Ret Val : random";
        }

        @Override
        public String join(String ip, String port) throws TException {
              try{
                BufferedReader br = new BufferedReader(new FileReader("./config.txt"));
                int count = 0;
                String st;
                while ((st = br.readLine()) != null){
                  if (count == 0){
                      try{
                          bit = Integer.parseInt(st);
                          bitSpan = (int)(Math.pow(2, bit));
                      }
                      catch(Exception e){
                        System.out.println("Invalid bit size in config file");                        
                      }
                      break;
                  }
                  ++count;
                }
              }
              catch(Exception e){}

             if(mutex == 0){
               synchronized(this){
                 int n = -1;
                 mutex = 1;

                 //computing random node to return
                 int s = arrListNodeDetails.size();
                 if(s!=0){
                    Random rand = new Random();
                    n = rand.nextInt(s);
                 }


                 NodeStruct newNode = new NodeStruct(ip, port);
                 ComputeHash hashVal = new ComputeHash();
                 int hashId = hashVal.hash(newNode.ipPort);
                 if(!setIds.contains(hashId)) setIds.add(hashId);
                 else{
                    hashId = (hashId+1)%bitSpan;       //hardcoded 32-bit
                    while(setIds.contains(hashId)){
                        hashId = (hashId+1)%bitSpan;
                    }
                    setIds.add(hashId);
                 }
                 newNode.id = hashId;

                 //code for avoiding duplicate entries in arraylist
                 int copyFlag = 0;
                 for(NodeStruct tempNode : arrListNodeDetails){
                    if(tempNode.ip == ip && tempNode.port == port){
                        copyFlag = 1;
                        break;
                    }
                 }
                 if(copyFlag == 0){
                    arrListNodeDetails.add(newNode);
                 }
                 copyFlag = 0;

                 if(n==-1){
                   return hashId +"_"+"Empty"+"_"+"Empty";
                 }
                 else{
                    return hashId +"_"+arrListNodeDetails.get(n).ip+"_"+arrListNodeDetails.get(n).port;
                 }

               }
             }
             else{
                return "Can't process Join - Synchronized with another Node";
             }
		    }


        @Override
        public String postJoin(String ip, String port) throws TException {
             mutex = 0;
             System.out.println("\nHitting PostJoin(): Mutex Set Back To :"+mutex);
             System.out.println("Joined Node to DHT: "+ip+" "+port);
             return "PostJoin() completed";
		    }

        //updateDHT(): Updates all the nodes, when a new node joins
        @Override
        public void updateDHT(int id, String ip, String port) throws TException {
          for (NodeStruct node:arrListNodeDetails) {
            if(node.id==id) continue;

            try {
              TTransport  transport = new TSocket(node.ip, Integer.valueOf(node.port));
              TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
              ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
              transport.open();
              client.updateList(id,ip+":"+port);
              transport.close();
            }
            catch(Exception e) {
              System.out.println("This Node was part of DHT but now has left: (restart supernode again and then join)");
            }
          }
             System.out.println("updateDHT Completed");
        }
}
