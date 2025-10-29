package request;

import java.util.List;
import java.util.Random;

public class GenerateSyntheticQuery {
    public static Random rand = new Random(11);
/*
SELECT * FROM SYNTHETIC MATCH_RECOGNIZE(
    ORDER BY eventTime
    MEASURES A.eventTime as ATS, B.eventTime as BTS, C.eventTime AS CTS
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN (A N1*? B N2*? C) WITHIN INTERVAL '1' MINUTE
    DEFINE
        A AS A.type = 'T_17' AND A.a1 <= 500,
        B AS B.type = 'T_15' AND B.a1 <= 500,
        C AS C.type = 'T_12' AND C.a1 <= 500
);
 */

    public static String[] allTypes =
            {"T_00", "T_01", "T_02", "T_03", "T_04",
            "T_05", "T_06", "T_07", "T_08", "T_09",
            "T_10", "T_11", "T_12", "T_13", "T_14",
            "T_15", "T_16", "T_17", "T_18", "T_19"};

    public static List<String> generateQueries(int varNum, int queryNum, double sel){

        int a1MaxValue = (int)(sel * 1000);

        for(int i = 0; i < queryNum; i++){
            String sql = "SELECT * FROM SYNTHETIC MATCH_RECOGNIZE(\n" +
                        "    ORDER BY eventTime\n" +
                        "    MEASURES";


            char start = 'A';
            for(int j = 0; j < varNum; j++){
                char curVarName = (char) (start + j);
                if(j == 0){
                    sql += " " + curVarName + ".eventTime as " + curVarName + "TS";
                }else{
                    sql += ", " + curVarName + ".eventTime as " + curVarName + "TS";
                }
            }
            sql += "\n    ONE ROW PER MATCH\n    AFTER MATCH SKIP TO NEXT ROW\n";
            sql += "    PATTERN(";
            for(int j = 0; j < varNum; j++){
                char curVarName = (char) (start + j);
                if(j == 0){
                    sql += curVarName;
                }else{
                    sql += " N" + j + "*? " + curVarName;
                }
            }
            sql += ") WITHIN INTERVAL '3' MINUTE\n    DEFINE\n        ";

            double hasEqualPro = 1.5 / (varNum - 1);
            int preTypeId = -1;
            // 20 types
            for(int j = 0; j < varNum; j++){
                char curVarName = (char) (start + j);
                int typeIdx = rand.nextInt(20);
                while(typeIdx == preTypeId){
                    typeIdx = rand.nextInt(20);
                }
                preTypeId = typeIdx;
                // A AS A.type = 'T_17' AND A.a1 <= 500,
                if(j == 0){
                    sql += curVarName + " AS " + curVarName + ".type = '" + allTypes[typeIdx] + "' AND " + curVarName + ".A1 <= " + a1MaxValue;
                }else{
                    sql += ",\n        " + curVarName + " AS " + curVarName + ".type = '" + allTypes[typeIdx] + "' AND " + curVarName + ".A1 <= " + a1MaxValue;
                    if(rand.nextDouble() < hasEqualPro){
                        char preVarName = (char) (start + j - 1);
                        String attrName = rand.nextInt(2) == 0 ? "A2" : "A3";
                        sql += " AND " + curVarName + "." + attrName + " = " + preVarName + "." + attrName;
                    }
                }
            }
            sql += "\n);";
            System.out.println(sql);
        }
        return null;
    }

    public static void main(String[] args){
        generateQueries(8, 50, 0.2);
    }
}
/*
默认是4分钟，20%的选择率，默认平均1.5个等值条件；

窗口大小实验：2，4，6，8
选择率：5% 10% 20% 30%

 */