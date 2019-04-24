import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.util.*;
import java.io.*;


/*
  Client Class:
  1. Requests for a random node from SuperNodeHandler
  2. Set <title, genre> to updateDHT
  3. Get GENRE for the requested TITLE
*/
public class Client {
    static String randomNode ="";
    static List<String> listOfFiles = new ArrayList<>();
    public static int portNumber = 0;
    public static String ip = "";
    static int flag = 0;
    public static void main(String [] args) {
            //Reading SuperNode details from config.txt
          if(ip.equals("")){
              try{
                BufferedReader br = new BufferedReader(new FileReader("./config.txt"));
                int count = 0;
                String st;
                while ((st = br.readLine()) != null){
                  if (count == 1){
                      String[] strArr = st.split(" ");
                      try{
                          ip = strArr[0];
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
          }


          try {
                System.out.println(ip+" "+portNumber);
                TTransport  transport = new TSocket(ip, portNumber);
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                SuperNodeInterface.Client client = new SuperNodeInterface.Client(protocol);
                transport.open();
                String str = client.getNode();
                String negativeMsg1 = "No node registered with SuperNode";
                String negativeMsg2 = "SuperNode busy: Try Again Later";
                if(!(str.equals(negativeMsg1)) && !(str.equals(negativeMsg2))){
                    randomNode = str;
                    flag = 1;
                }
                else{
                  System.out.println(str);
                }
                transport.close();
          } catch(TException e) {System.out.println(e);}


        if(flag == 1){
          int len = args.length;
          if(len == 0){
              System.out.println("provide set or get argument on the command line");
              System.out.println("eg: java -cp \".:/usr/local/Thrift/*\" Client set <filename>");
              System.out.println("eg: java -cp \".:/usr/local/Thrift/*\" Client get <title>\n");
          }
          else {
            if(args[0].equals("set")) {
              try{
                String filename = args[1];
                BufferedReader br = new BufferedReader(new FileReader(filename));
                String st;
                  while ((st = br.readLine()) != null){
                      String[] splitStr = st.split(":");
                      String title = splitStr[0];
                      String genre = splitStr[1];
                      String[] conn = randomNode.split(":");
                      TTransport transport = new TSocket(conn[0], Integer.valueOf(conn[1]));
                      TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                      ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                      transport.open();
                      client.setGenre(title, genre);
                      transport.close();
                  }
              }
              catch(Exception e){
                System.out.println("Error in parsing text file / File_Not_Found");
              }
            }
            if(args[0].equals("get")) {
              try{
                int l = args.length;
                String str = "";
                for(int i = 1; i<len; i++){
                  str = str + args[i] + " ";
                }
                str = str.trim();

                String[] conn = randomNode.split(":");
                TTransport transport = new TSocket(conn[0], Integer.valueOf(conn[1]));
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                ClientNodeInterface.Client client = new ClientNodeInterface.Client(protocol);
                transport.open();
                String genreRet = client.getGenre(str, "");
                System.out.println(genreRet);
                transport.close();
              }
              catch(Exception e){
                System.out.println("Error in parsing get req: \n"+e);
              }
            }
          }
        }
    }
}
