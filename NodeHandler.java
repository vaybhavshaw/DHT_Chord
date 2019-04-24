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


// Finger Structure of a Finger Table
class Finger{
    int start;
    int interval_start;
    int interval_end;
    String successor;
    int successorId;
}

//NodeHandler(): Provides the implementation of node
public class NodeHandler implements ClientNodeInterface.Iface{
        static ArrayList<Finger> fingerTable;
        static NodeStruct node;
        static HashMap<String, String> bookMap;
        static TreeMap<Integer,String> map;
        static HashSet<String> set;
        static int count=0;
        static int bit = 0;
        static int bitSpan = 0;
        static int portNumberSN = 0;
        static String ipSN = "";

        @Override
        public boolean ping() throws TException {
             NodeHandler handler = new NodeHandler();
			       return true;
		    }

        //getGenre(): Return the Genre for a given Book Title
        @Override
        public String getGenre(String title, String traversal) throws TException {
          traversal = traversal + node.id + " --> ";
          NodeHandler hclient = new NodeHandler();
          ComputeHash hashVal = new ComputeHash();
          int h = hashVal.hash(title);

          if(node.id==h || Integer.valueOf(hclient.findSuccessor(h).split("_")[1])==node.id){
            String ret= bookMap.get(title);
            System.out.println("\nTraversal for "+ title +" is: "+ traversal+" end ");
            return ret==null?"Title Not in DHT / DHT not Set":"Genre: "+ret;
          }

          for(int i=0;i<bit;i++){
            if(fingerTable.get(i).start==h){
              try {
                    String[] strSplit = fingerTable.get(i).successor.split(":");
                    TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                    TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                    ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                    transport.open();
                    String ret=client.getInNode(title, traversal);
                    transport.close();
                    return ret;
              }catch(TException e)
              {
                System.out.println(e);
              }
            }
          }

          if(h>fingerTable.get(0).start && h<=fingerTable.get(0).successorId){
            try {
                  String[] strSplit = fingerTable.get(0).successor.split(":");
                  TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                  TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                  ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                  transport.open();
                  String ret=client.getInNode(title, traversal);
                  transport.close();
                  return ret;
            }catch(TException e) {System.out.println(e);}
          }

          for(int i=0;i<bit-1;i++){
            if(inRange3(h,fingerTable.get(i).start,fingerTable.get(i+1).start)){
              try {
                    String[] strSplit = fingerTable.get(i).successor.split(":");
                    TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                    TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                    ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                    transport.open();
                    String ret=client.getGenre(title, traversal);
                    transport.close();
                    return ret;
              }catch(TException e) {System.out.println(e);}
          }
        }

        try {
              String[] strSplit = fingerTable.get(bit-1).successor.split(":");  //m-1
              TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
              TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
              ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
              transport.open();
              String ret= client.getGenre(title, traversal);
              transport.close();
              return ret;
        }catch(TException e) {System.out.println(e);}
       return "";
      }

      //getInNode(): Gets the Genre for a given Book Title : Called from getGenre()
      @Override
      public String getInNode(String title, String traversal) throws TException {
          String ret= bookMap.get(title);
          System.out.println("\nTraversal for "+title+" is: "+ traversal+node.id);
          return ret==null?"Title Not in DHT / DHT not Set":"Genre: "+ret;
      }

      //setInNode(): Sets the Genre for a given Book Title : Called from setGenre()
      @Override
      public String setInNode(String title, String genre , int h) throws TException {
          bookMap.put(title,genre);
          ++count;
          System.out.println("Book Title: "+title+"\tHash_Of_The_Book :"+h);
          return "";
      }

        //setGenre(): Sets the genre for a given Book Title
        @Override
        public String setGenre(String title, String genre) throws TException {

             NodeHandler hclient = new NodeHandler();
             ComputeHash hashVal = new ComputeHash();
             int h = hashVal.hash(title);

             if(node.id==h || Integer.valueOf(hclient.findSuccessor(h).split("_")[1])==node.id){
               bookMap.put(title,genre);
               ++count;
               System.out.println("Book Title: "+title+"\tHash_Of_The_Book :"+h);
               return "" ;
             }


             for(int i=0;i<bit;i++){
               if(fingerTable.get(i).start==h){
                 try {
                       String[] strSplit = fingerTable.get(i).successor.split(":");
                       TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                       TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                       ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                       transport.open();
                       client.setInNode(title,genre,h);
                       transport.close();
                       return "";
                 }catch(TException e)
                 {

                   System.out.println(e);
                 }
               }
             }

             if(h>fingerTable.get(0).start && h<=fingerTable.get(0).successorId){
               try {
                     String[] strSplit = fingerTable.get(0).successor.split(":");
                     TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                     TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                     ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                     transport.open();
                     client.setInNode(title,genre,h);
                     transport.close();
                     return "";
               }catch(TException e) {System.out.println(e);}
             }

             for(int i=0;i<bit-1;i++){
               if(inRange3(h,fingerTable.get(i).start,fingerTable.get(i+1).start)){
                 try {
                       String[] strSplit = fingerTable.get(i).successor.split(":");
                       TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                       TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                       ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                       transport.open();
                       client.setGenre(title,genre);
                       transport.close();
                       return "";
                 }catch(TException e) {System.out.println(e);}
             }
           }

           try {
                 String[] strSplit = fingerTable.get(bit-1).successor.split(":");  //m-1
                 TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                 TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                 ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                 transport.open();
                 client.setGenre(title,genre);
                 transport.close();
                 return "";
           }catch(TException e) {System.out.println(e);}

          return "";
		    }

        public static boolean inRange3(int id, int start, int end){
          if(start<end){
            if(id>start && id<end) return true;
            else return false;
          }
          else{
            if((id>start && id<=bitSpan-1) || (id>=0 && id<end ))return true;
             else return false;
          }
        }

        @Override
        public void setPredecessor(String ipPort){
          node.predecessor=ipPort;
        }

        @Override
        public String getPredecessor(){
          return node.predecessor;
        }

        @Override
        public String getSuccessor(){
          return node.successor;
        }

        @Override
        public int getSuccessorId(){
          return node.successorId;
        }

        @Override
        public void setSuccessor(String ipPort){
          node.successor = ipPort;
        }

        @Override
        public void setSuccessorId(int id){
          node.successorId = id;
        }

        @Override
        public int getId(){
          return node.id;
        }

        @Override
        public String getIpPort(){
          return node.ipPort;
        }

        //connectToSuperNode(): Send a connection Req to SuperNode for joining
        public void connectToSuperNode(String ip, String port){

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
              }
              else if (count == 1){
                  String[] strArr = st.split(" ");
                  try{
                      ipSN = strArr[0];
                      portNumberSN = Integer.parseInt(strArr[1]);
                  }
                  catch(Exception e){
                    System.out.println("Port Number should be a valid Integer");
                    return;
                  }
              }
              ++count;
              }
            }
            catch(Exception e){}

            node = new NodeStruct(ip,port);
            fingerTable = new ArrayList<>();
            bookMap = new HashMap<>();
            map = new TreeMap<>();
            set = new HashSet<>();
            String str="";

            try{
              TTransport  transport = new TSocket(ipSN, portNumberSN);
              TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
              SuperNodeInterface.Client client = new SuperNodeInterface.Client(protocol);
              transport.open();
              str = client.join(ip,port);
              transport.close();

            } catch(TException e) {System.out.println(e);}

            String[] strSplit = str.split("_");
            node.id=Integer.valueOf(strSplit[0]);
            map.put(node.id,ip+":"+port);
            if(!strSplit[1].equals("Empty")){
              String retList="";
              try{
                TTransport  transport = new TSocket(strSplit[1], Integer.valueOf(strSplit[2]));
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                transport.open();
                retList = client.getNodeList();
                transport.close();
              } catch(TException e) {System.out.println(e);}

              String[] retSplit = retList.split("_");
              int len= retSplit.length;

              for(int i=0;i<len-1;i+=2){
                map.put(Integer.valueOf(retSplit[i]),retSplit[i+1]);
              }
            }

            if(strSplit[1].equals("Empty")){
              Finger f;
              for(int i=0;i<bit;i++){
                f = new Finger();
                f.start = (node.id+(int)Math.pow(2,i))%bitSpan;
                f.interval_start = f.start;
                f.interval_end = (f.interval_start+(int)Math.pow(2,i)-1)%bitSpan;
                f.successor = node.ip+":"+node.port;
                f.successorId = node.id;
                fingerTable.add(f);
              }
              node.predecessor=node.ipPort;
              node.successor=node.ipPort;
              node.successorId=node.id;
            }
            else{
              init_finger_table(strSplit[1]+":"+strSplit[2]);
              update_others();
            }

          NodeHandler printHandler = new NodeHandler();
          printHandler.printDetails();

          //Implementing PostJoin()
          try {
            TTransport  transport = new TSocket(ipSN, portNumberSN);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            SuperNodeInterface.Client client = new SuperNodeInterface.Client(protocol);
            transport.open();
            String postJoinStr = client.postJoin(ip,port);
            transport.close();
          }
          catch(Exception e) {
            System.out.println("Exception in PostJoin : "+e);
          }

          //Go update DHT
          try {
            TTransport  transport = new TSocket(ipSN, portNumberSN);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            SuperNodeInterface.Client client = new SuperNodeInterface.Client(protocol);
            transport.open();
            client.updateDHT(node.id, node.ip, node.port);
            transport.close();
          }
          catch(Exception e) {
            System.out.println("Exception in updateDHT : "+e);
          }
        }

        //getNodeList(): Gets the node list of current node
        @Override
        public String getNodeList(){
          StringBuilder sb= new StringBuilder();
          for (Map.Entry<Integer, String> entry : map.entrySet()) {
            sb.append(entry.getKey()).append("_").append(entry.getValue()).append("_");
          }
          return sb.toString();
        }

        //updateList(): Updates own node list as a part of updateDHT
        @Override
        public void updateList(int id, String ipPort){
          map.put(id,ipPort);
        }

        //printDetails(): Print the node and fingertable details
        @Override
        public void printDetails(){
            System.out.println("\nNode Id: "+node.id);
            System.out.println("IP and Port Details: "+node.ipPort);
            System.out.println("Predecessor of the Node: "+node.predecessor);
            System.out.println("Successor of the Node: "+node.successor);
            System.out.println("Printing the Finger Table :");
            for(Finger f : fingerTable){
              System.out.print("start: "+f.start+"\t");
              System.out.print("interval_start: "+f.interval_start+"\t");
              System.out.print("interval_end: "+f.interval_end+"\t");
              System.out.print("successor: "+f.successor+"\t");
              System.out.print("successorId: "+f.successorId+"\t");
              System.out.println();
            }
        }

        //range(): finds the correct range: supports method implementations
        public static boolean range(int start, int lowoffset, int end, int highoffset, int arg)
        {
          if(start == end)
          {
            boolean b = arg!=start;
            if(lowoffset == highoffset && lowoffset == 0) return b;
            else return true;
          }
          else
          {
            if (start<end) return ((arg >(start-lowoffset)) && (arg<(end+highoffset)));
            else return (((start-lowoffset)<arg) || (arg<(end+highoffset)));
          }
        }

        @Override
        public String findSuccessor(int id){
          if(map.containsKey(id)){
            return map.get(id)+"_"+id;
          }
          Integer val=map.ceilingKey(id);
          if(val==null){
            val = map.firstKey();
            return map.get(val)+"_"+val;
          }
          return map.get(val)+"_"+val;
        }

        @Override
        public String findPredecessor(int id){

          if(map.containsKey(id))
            return map.get(id);

          Integer val=map.lowerKey(id);
          if(val==null){
            val = map.lastKey();
            return map.get(val);
          }
            return map.get(val);
        }


       //closestPrecedingFinger(): Finds the closest preceding finger for a given id
       @Override
        public String closestPrecedingFinger(int id){
          for(int i=bit-1;i>=0;i--){
            if (range(node.id,0,id,0,fingerTable.get(i).successorId)){
              return fingerTable.get(i).successor;
            }
          }
          return node.ipPort;
        }

       //init_finger_table(): Initializes the finger table of the newly joined node
       public static void init_finger_table(String randomIpPort){
         NodeHandler hclient = new NodeHandler();
         String[] strSplit = randomIpPort.split(":");
         String retSuccessor="";
         int start = (node.id+(int)Math.pow(2,0))%bitSpan;
         retSuccessor = hclient.findSuccessor(start);
         strSplit = retSuccessor.split("_");
         Finger f = new Finger();
         f.start = (node.id+(int)Math.pow(2,0))%bitSpan;
         f.interval_start = f.start;
         f.interval_end = (f.interval_start+(int)Math.pow(2,0)-1)%bitSpan;
         f.successor = strSplit[0];
         f.successorId = Integer.valueOf(strSplit[1]);
         fingerTable.add(f);
         node.successor=f.successor;
         node.successorId=f.successorId;
         strSplit = f.successor.split(":");
         try{
           TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
           TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
           ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
           transport.open();
           node.predecessor=client.getPredecessor();
           client.setPredecessor(node.ipPort);
           transport.close();
         }
         catch(Exception e){System.out.println(e);}

         for(int i=0;i<bit-1;i++){
           f= new Finger();
           f.start = (node.id+(int)Math.pow(2,i+1))%bitSpan;
           f.interval_start = f.start;
           f.interval_end = (f.interval_start+(int)Math.pow(2,i+1)-1)%bitSpan;
           if (range(node.id, 1, fingerTable.get(i).successorId, 0, f.start)) {
              f.successor=fingerTable.get(i).successor;
              f.successorId=fingerTable.get(i).successorId;
            }
            else{
              strSplit=randomIpPort.split(":");
              try{
                TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                transport.open();
                retSuccessor = hclient.findSuccessor(f.start);
                transport.close();
              }
              catch(Exception e){System.out.println(e);}
              strSplit = retSuccessor.split("_");
              f.successor = strSplit[0];
              f.successorId = Integer.valueOf(strSplit[1]);
            }
            fingerTable.add(f);
         }
           NodeHandler handler = new NodeHandler();
           handler.printDetails();
      }

       //update_others(): Update the finger tables of the other nodes
       public static void update_others(){
         for(int i=0;i<bit;i++){
           int val = node.id - (int)Math.pow(2,i);
           if(val<0) val+=bitSpan;
           NodeHandler updateHandler = new NodeHandler();
           String retPredecessor = updateHandler.findPredecessor(val);
           if(retPredecessor.equals(node.ipPort)){
             break;
           }
           String[] strSplit = retPredecessor.split(":");
           try{
             TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
             TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
             ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
             transport.open();
             client.update_finger_table(node.id+"_"+node.ipPort,i);
             transport.close();
           }
           catch(Exception e){System.out.println(e);}
         }
       }

       //update_finger_table(): Recursive function to update the finger table of the other nodes
       @Override
       public void update_finger_table(String s, int i){
          String[] strSplit = s.split("_");
          if (range(node.id,1,fingerTable.get(i).successorId,0,Integer.valueOf(strSplit[0]))){
          if(map.containsKey(fingerTable.get(i).start)){
            fingerTable.get(i).successor=map.get(fingerTable.get(i).start);
            fingerTable.get(i).successorId=fingerTable.get(i).start;
          }
          else{
            fingerTable.get(i).successor=strSplit[1];
            fingerTable.get(i).successorId=Integer.valueOf(strSplit[0]);
          }

            if(i==0){
              node.successor=strSplit[1];
              node.successorId=Integer.valueOf(strSplit[0]);
            }

            NodeHandler handler = new NodeHandler();
            handler.printDetails();
            String p = node.predecessor;
            strSplit = p.split(":");
            try{
                TTransport transport = new TSocket(strSplit[0], Integer.valueOf(strSplit[1]));
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                if (!transport.isOpen()) transport.open();
                client.update_finger_table(s,i);
                transport.close();
            }
            catch(Exception e){}

          }
       }
}
