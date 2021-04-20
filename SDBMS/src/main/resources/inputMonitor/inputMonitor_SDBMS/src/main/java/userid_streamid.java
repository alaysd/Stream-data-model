import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.json.*;
import sun.awt.X11.XSystemTrayPeer;

import javax.swing.plaf.nimbus.State;
import java.io.FileReader;
import java.util.*;
import java.sql.*;

public class userid_streamid {

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

            String dataTableName = parameters.getString("streamName") + "_data";

            //...Last record read
            int lastCount = 0;
            int[] attrNumberOfRequired = new int[reqAttrs.length()];
            try {


                int j = 0;
                for(int i = 0 ; i < dataAttrs.length() && j < reqAttrs.length() ; i++) {
                    System.out.println(dataAttrs.getString(i) + " :: " + reqAttrs.getString(j));
                    if(dataAttrs.getString(i).equals(reqAttrs.getString(j))) {
                        attrNumberOfRequired[j] = i;
                        j++;
                    }
                }


                // CREATING DATA TABLE;
                Class.forName("com.mysql.cj.jdbc.Driver");
                Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/sdbms?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC","alay","password");
                Statement stmt=con.createStatement();


                String sqlQuery = "CREATE TABLE " + dataTableName + " ( ";
                for(int k = 0 ; k < reqAttrs.length() ; k++) {
                    sqlQuery += reqAttrs.getString(k) + " " + reqAttrsDatType.getString(k) + ",";
                }
                sqlQuery += "insertTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP )";

                System.out.println(sqlQuery);

                stmt.executeUpdate(sqlQuery);
                con.close();

                // DATA TABLE CREATED

            } catch (Exception e) {
                System.out.println("ERRORRRR : " + e);
            }


            CSVParser csvParser = new CSVParserBuilder().withSeparator(',').build();
            while(true) {

                if(windowType.equals("Tumbling")) {
                    try(CSVReader reader = new CSVReaderBuilder(new FileReader("/home/alay/workspace/"+dataFileSrc))
                            .withCSVParser(csvParser)
                            .withSkipLines(lastCount)
                            .build()) {
                        System.out.println("Tumbling");

                        Class.forName("com.mysql.cj.jdbc.Driver");
                        Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/sdbms?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC","alay","password");


                        long startTime = System.currentTimeMillis();

                        int i;
                        for(i = 0 ; i < windowSize ; i++) {
                            Connection con1=DriverManager.getConnection("jdbc:mysql://localhost:3306/sdbms?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC","alay","password");

                            String[] tuple = reader.readNext();
                            if(tuple==null) break;

                            for (String attribute:
                                    tuple) {
                                System.out.print(attribute + " ");
                            }
                            String valuesToInsert = "";
                            int currReqIndex = 0;
                            System.out.println();

                            for(int j = 0 ; j < tuple.length && currReqIndex < attrNumberOfRequired.length; j++) {
                                if(j == attrNumberOfRequired[currReqIndex]) {

                                    valuesToInsert += "\"" + tuple[j] + "\"";
                                    if(currReqIndex != attrNumberOfRequired.length-1) {
                                        valuesToInsert += ",";
                                    }
                                    currReqIndex++;
                                }
                            }

                            System.out.println("valuesToInsert" + valuesToInsert);


                            System.out.println();

                            // Insert the tuples in database;
                            Statement stmt=con1.createStatement();

                            String insertTuple = "INSERT INTO " + dataTableName + "(";

                            for(int itq = 0 ; itq < reqAttrs.length() ; itq++) {
                                insertTuple += reqAttrs.getString(itq);
                                if(itq != reqAttrs.length() - 1) {
                                    insertTuple += ",";
                                }
                            }

                            insertTuple += " ) values  (" + valuesToInsert + " )";

                            System.out.println(insertTuple);

                            try{
                                stmt.executeUpdate(insertTuple);
                            } catch (Exception e) {
                                System.out.println("Error insert into data table at row " + i + " error "  + e);
                            }

                            con1.close();

                        }

                        // Delete old data from table;

                        Statement countData = con.createStatement();
                        String queryCount = "select count(*) from " + dataTableName;
                        ResultSet rsQueryCount = countData.executeQuery(queryCount);
                        rsQueryCount.next();

                        int haha = rsQueryCount.getInt("count(*)");
                        if(haha > 2*windowSize) {
                            String deleteQuery = "DELETE FROM " + dataTableName + " ORDER BY insertTime LIMIT " + String.valueOf(haha - 2*windowSize);
                            Statement deleteStmt = con.createStatement();
                            try {
                                deleteStmt.executeUpdate(deleteQuery);
                            } catch (Exception e) {
                                System.out.println("ERRROR deleting records " + e );
                            }
                        }

                        Statement stmt=con.createStatement();

                        if(i != 0) {
                            for(int k = 0 ; k < tableNames.length() ; k++) {

                                try {
                                    String dropSql = "drop table " + tableNames.getString(k);
                                    stmt.executeUpdate(dropSql);
                                } catch (Exception e) {
                                    System.out.println("Unable to drop " + e);
                                }
                                try {
                                    String createSql = "CREATE TABLE " + tableNames.getString(k) + " AS " + queries.getString(k);
                                    System.out.println(createSql);
                                    stmt.executeUpdate(createSql);
                                } catch (Exception e) {
                                    System.out.println("Unable to create and insert " + e);
                                }
                            }
                        }

                        con.close();


                        System.out.println("======================");
                        lastCount += i;
                        System.out.println(lastCount);

                        long stopTime = System.currentTimeMillis();

                        if((stopTime-startTime) <= windowVelocity*1000) {
                            Thread.sleep(windowVelocity*1000 - (stopTime-startTime));
                        }

                    } catch (Exception e) {
                        System.out.println("Error in while(true) " + e);
                    }

                } else if(windowType.equals("Sliding")) {
                    try (CSVReader reader = new CSVReaderBuilder(new FileReader("/home/alay/workspace/"+dataFileSrc))
                            .withCSVParser(csvParser)
                            .build()){
                        long startTime = System.currentTimeMillis();
                        List<String[]> allData = reader.readAll();

                        Class.forName("com.mysql.cj.jdbc.Driver");
                        Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/sdbms?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC","alay","password");


                        for(int i = Math.max(0, (int)allData.size() - (int)windowSize) ; i < allData.size() ; i++) {
                            for(String attribute : allData.get(i)) {
                                System.out.print(attribute + " ");
                            }
                            Connection con1=DriverManager.getConnection("jdbc:mysql://localhost:3306/sdbms?allowPublicKeyRetrieval=true&useSSL=false&serverTimezone=UTC","alay","password");

                            //////////////////////////
                            String valuesToInsert = "";
                            int currReqIndex = 0;
                            System.out.println();

                            for(int j = 0 ; j < allData.get(i).length && currReqIndex < attrNumberOfRequired.length; j++) {
                                if(j == attrNumberOfRequired[currReqIndex]) {

                                    valuesToInsert += "\"" + allData.get(i)[j] + "\"";
                                    if(currReqIndex != attrNumberOfRequired.length-1) {
                                        valuesToInsert += ",";
                                    }
                                    currReqIndex++;
                                }
                            }

                            System.out.println("valuesToInsert" + valuesToInsert);


                            System.out.println();

                            // Insert the tuples in database;
                            Statement stmt=con1.createStatement();

                            String insertTuple = "INSERT INTO " + dataTableName + "(";

                            for(int itq = 0 ; itq < reqAttrs.length() ; itq++) {
                                insertTuple += reqAttrs.getString(itq);
                                if(itq != reqAttrs.length() - 1) {
                                    insertTuple += ",";
                                }
                            }

                            insertTuple += " ) values  (" + valuesToInsert + " )";

                            System.out.println(insertTuple);

                            try{
                                stmt.executeUpdate(insertTuple);
                            } catch (Exception e) {
                                System.out.println("Error insert into data table at row " + i + " error "  + e);
                            }

                            con1.close();

                            System.out.println();
                        }

                        Statement countData = con.createStatement();
                        String queryCount = "select count(*) from " + dataTableName;
                        ResultSet rsQueryCount = countData.executeQuery(queryCount);
                        rsQueryCount.next();

                        int haha = rsQueryCount.getInt("count(*)");
                        if(haha > windowSize) {
                            String deleteQuery = "DELETE FROM " + dataTableName + " ORDER BY insertTime LIMIT " + String.valueOf(haha - windowSize);
                            Statement deleteStmt = con.createStatement();
                            try {
                                deleteStmt.executeUpdate(deleteQuery);
                            } catch (Exception e) {
                                System.out.println("ERRROR deleting records " + e );
                            }
                        }

                        Statement stmt=con.createStatement();


                        for(int k = 0 ; k < tableNames.length() ; k++) {

                            try {
                                String dropSql = "drop table " + tableNames.getString(k);
                                stmt.executeUpdate(dropSql);
                            } catch (Exception e) {
                                System.out.println("Unable to drop " + e);
                            }
                            try {
                                String createSql = "CREATE TABLE " + tableNames.getString(k) + " AS " + queries.getString(k);
                                System.out.println(createSql);
                                stmt.executeUpdate(createSql);
                            } catch (Exception e) {
                                System.out.println("Unable to create and insert " + e);
                            }
                        }


                        con.close();

                        System.out.println("=========================");

                        long stopTime = System.currentTimeMillis();

                        if((stopTime-startTime) <= windowVelocity*1000) {
                            Thread.sleep(windowVelocity*1000 - (stopTime-startTime));
                        }


                    } catch (Exception e) {
                        System.out.println("Sliding exception " + e);
                    }
                } else if (windowType.equals("Hopping")) {
                    try(CSVReader reader = new CSVReaderBuilder(new FileReader("/home/alay/workspace/"+dataFileSrc))
                            .withCSVParser(csvParser)
                            .withSkipLines(lastCount)
                            .build()) {
                        System.out.println("Tumbling");

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
                        int hopSize = 0;
                        lastCount += hopSize;

                        long stopTime = System.currentTimeMillis();

                        if((stopTime-startTime) <= windowVelocity*1000) {
                            Thread.sleep(windowVelocity*1000 - (stopTime-startTime));
                        }

                    } catch (Exception e) {
                        System.out.println("Error in while(true) " + e);
                    }
                }

            }

        } catch (Exception e) {
            System.out.println("ERROR : " + e);
        }



    }
}
