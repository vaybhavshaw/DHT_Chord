import java.util.Arrays;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.io.*;
import java.lang.*;

public class ComputeHash{
    static int bit = -1;
    static int bitSpan = 0;
    public static int hash(String s){
            try{
              if(bit<0){
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
            }
            catch(Exception e){}
            MessageDigest digest = null;
            try
            {
                digest = MessageDigest.getInstance("SHA-1");
            }
            catch (NoSuchAlgorithmException e) {
                System.out.println(e);
            }
            digest.reset();
            digest.update(s.getBytes());
            byte[] arr = digest.digest();
            int hashVal = Math.abs(new BigInteger(arr).intValue()%bitSpan); // 32-bit harcoded : change
            return hashVal;
        }
}
