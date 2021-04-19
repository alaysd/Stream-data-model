import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.json.*;
import sun.awt.X11.XSystemTrayPeer;

import java.io.FileReader;
import java.util.*;
import java.sql.*;

public class userid_streamid {

    public int createTableInDB(String query) {
        try {



            return 1;
        } catch (Exception E) {
            System.out.println("createTableInDB : "+E);
            return -1;
        }
    }


    public int deleteFromDB(String query) {
        return -1;
    }

    public static void main(String[] args) {
        System.out.println("Arguements " + args[0]);

        try {
            JSONObject parameters = new JSONObject(args[0]);
            //...

            //...

            JSONArray tableNames = parameters.getJSONArray("tableNames");
            JSONArray queries = parameters.getJSONArray("queries");
            JSONArray dataAttrs = parameters.getJSONArray("dataAttributes");
            JSONArray reqAttrs = parameters.getJSONArray("requiredAttributes");
            JSONArray reqAttrsDatType = parameters.getJSONArray("requiredAttributesDataType");

            long windowSize = parameters.getLong("windowSize");
            long windowVelocity = parameters.getLong("windowVelocity");
            String windowType = parameters.getString("windowType");
            String dataSrc = parameters.getString("dataSrc");
            String dataFileSrc = parameters.getString("dataFileSrc");

            //...Last record read
            int lastCount = 0;

            try {

                int[] attrNumberOfRequired = new int[reqAttrs.length()];
                int j = 0;
                for(int i = 0 ; i < dataAttrs.length() && j < reqAttrs.length() ; i++) {
                    if(dataAttrs.getString(i).equals(reqAttrs.getString(j))) {
                        attrNumberOfRequired[j] = i;
                        j++;
                    }
                }

                // creating required  tables;
                Class.forName("com.mysql.cj.jdbc.Driver");
                Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/test1?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC","alay","password");

                for(int i = 0 ; i < tableNames.length() ; i++) {

                    String curr = tableNames.getString(i);
                    Statement stmt=con.createStatement();

                    String sqlQuery = "create table " + curr + " ( ";
                    for(int k = 0 ; k < reqAttrs.length()-1; k++) {
                        sqlQuery += reqAttrs.getString(k) + " " + reqAttrsDatType.getString(k) + ",";
                    }
                    sqlQuery += reqAttrs.getString(reqAttrs.length()-1) + " " + reqAttrsDatType.getString(reqAttrs.length()-1) + " ) ";
                    System.out.println(sqlQuery);
                    stmt.executeUpdate(sqlQuery);

                    // Closing connection;
                    if(con!=null) {
                        con.close();
                    }

                }

            } catch (Exception e) {
                System.out.println("ERRORRRR : " + e);
            }


            CSVParser csvParser = new CSVParserBuilder().withSeparator(',').build();
            while(true) {

                try(CSVReader reader = new CSVReaderBuilder(new FileReader("/home/alay/workspace/"+dataFileSrc))
                        .withCSVParser(csvParser)
                        .withSkipLines(lastCount)
                        .build()) {
                    long startTime = System.currentTimeMillis();
                    int i;
                    for(i = 0 ; i < windowSize ; i++) {
                        String[] tuple = reader.readNext();
                        if(tuple==null) break;
                        for (String attribute:
                             tuple) {
                            System.out.print(attribute + " ");
                        }
                        System.out.println();

                        // Insert the tuples in database;
                        // Delete old data from table;

                    }
                    System.out.println("======================");



                    lastCount += i;



                    long stopTime = System.currentTimeMillis();

                    if((stopTime-startTime) <= windowVelocity*1000) {
                        Thread.sleep(windowVelocity*1000 - (stopTime-startTime));
                    }

                } catch (Exception e) {
                    System.out.println("Error in while(true) " + e);
                }






            }






        } catch (Exception e) {
            System.out.println("ERROR : " + e);
        }



    }
}
