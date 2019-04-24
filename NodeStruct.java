public class NodeStruct{
    String ip;
    String port;
    String ipPort;
    int id;
    String predecessor;
    String successor;
    int successorId;
    NodeStruct(){}
    NodeStruct(String ip, String port){
          this.ip = ip;
          this.port = port;
          this.ipPort = this.ip+":"+this.port;
    }
}
