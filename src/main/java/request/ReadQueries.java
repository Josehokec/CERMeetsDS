package request;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import java.io.File;

public class ReadQueries {
    static String sep = File.separator;

    public static List<String> readQueries(String filePath){
        List<String> sqlList = new ArrayList<>(300);
        StringBuilder sql = new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if(line.contains(";")){
                    sqlList.add(sql + line);
                    sql = new StringBuilder();
                }else{
                    sql.append(line).append("\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace(); // 处理异常
        }

        return sqlList;
    }

    public static List<String> getQueryList(String datasetName, boolean isEsper){
        switch(datasetName){
            case "CRIMES":
                return isEsper ? getCrimesEsperQueryList() : getCrimesQueryList();
            case "CITIBIKE":
                return isEsper ? getCitibikeEsperQueryList() : getCitibikeQueryList();
            case "CLUSTER":
                return isEsper ? getClusterEsperQueryList() : getClusterQueryList();
            case "SYNTHETIC":
                return isEsper ? getSyntheticEsperQueryList() : getSyntheticQueryList();
            default:
                throw new RuntimeException("without this dataset information, you need to initialize it...");
        }
    }

    public static List<String> getCrimesQueryList(){
        String filePath = System.getProperty("user.dir") + sep + "src"
                + sep + "main" + sep + "java" + sep + "request" + sep + "crimes_query.txt";
        return readQueries(filePath);
    }

    // esper
    public static List<String> getCrimesEsperQueryList(){
        String filePath = System.getProperty("user.dir") + sep + "src"
                + sep + "main" + sep + "java" + sep + "request" + sep + "crimes_query_esper.txt";
        List<String> originalQueries = readQueries(filePath);
        List<String> esperQueries = new ArrayList<>(originalQueries.size());
        // remove ';'
        for (String query : originalQueries) {
            esperQueries.add(query.substring(0, query.length() - 1));
        }
        return esperQueries;
    }

    public static List<String> getCitibikeQueryList(){
        String filePath = System.getProperty("user.dir") + sep + "src"
                + sep + "main" + sep + "java" + sep + "request" + sep + "citibike_query.txt";
        return readQueries(filePath);
    }

    // esper
    public static List<String> getCitibikeEsperQueryList(){
        String filePath = System.getProperty("user.dir") + sep + "src"
                + sep + "main" + sep + "java" + sep + "request" + sep + "citibike_query_esper.txt";
        List<String> originalQueries = readQueries(filePath);
        List<String> esperQueries = new ArrayList<>(originalQueries.size());
        // remove ';'
        for (String query : originalQueries) {
            esperQueries.add(query.substring(0, query.length() - 1));
        }
        return esperQueries;
    }

    public static List<String> getClusterQueryList(){
        String filePath = System.getProperty("user.dir") + sep + "src"
                + sep + "main" + sep + "java" + sep + "request" + sep + "cluster_query.txt";
        return readQueries(filePath);
    }

    public static List<String> getClusterEsperQueryList(){
        String filePath = System.getProperty("user.dir") + sep + "src"
                + sep + "main" + sep + "java" + sep + "request" + sep + "cluster_query_esper.txt";
        List<String> originalQueries = readQueries(filePath);
        List<String> esperQueries = new ArrayList<>(originalQueries.size());
        // remove ';'
        for (String query : originalQueries) {
            esperQueries.add(query.substring(0, query.length() - 1));
        }
        return esperQueries;
    }

    public static List<String> getSyntheticQueryList(){
        String filePath = System.getProperty("user.dir") + sep + "src"
                + sep + "main" + sep + "java" + sep + "request" + sep + "synthetic_query.txt";
        return readQueries(filePath);
    }

    public static List<String> getSyntheticEsperQueryList(){
        String filePath = System.getProperty("user.dir") + sep + "src"
                + sep + "main" + sep + "java" + sep + "request" + sep + "synthetic_query_esper.txt";
        List<String> originalQueries = readQueries(filePath);
        List<String> esperQueries = new ArrayList<>(originalQueries.size());
        // remove ';'
        for (String query : originalQueries) {
            esperQueries.add(query.substring(0, query.length() - 1));
        }
        return esperQueries;
    }

//    public static void main(String[] args){
//        //getCrimesQueryList();
//        getCitibikeEsperQueryList();
//    }
}

