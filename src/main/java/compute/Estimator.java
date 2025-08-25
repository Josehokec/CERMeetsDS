package compute;

public class Estimator {
    public static int savedMaxCategoryNum = 15;

    public static double calculate(double[] pros, int sumCategoryNum, double averageEventNum){
        int caseNum = pros.length;
        double keySum = 0;
        double proSum = 0;
        for (double pro : pros) {
            proSum += pro;
            keySum += (1 - Math.pow(1 - pro, averageEventNum));
        }
        if(sumCategoryNum > caseNum){
            int remainingCategoryNum = sumCategoryNum - savedMaxCategoryNum;
            double avgRemainingPro = (1 - proSum) / remainingCategoryNum;
            keySum += remainingCategoryNum * (1 - Math.pow(1 - avgRemainingPro, averageEventNum));
        }
        return keySum;
    }

    public static int calClusterJobId(double estimateEventNum, double windowIdNum){
        double rowNum = 144647998;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] counts = {
                16327120, 9066268, 8541727, 6650098, 2149362,
                1366014, 895739, 767469, 733288, 508925,
                448947, 418236, 351817, 301867, 299843

        };

        int jobIDCategoryNum = 672004;
        double[] pros = new double[savedMaxCategoryNum];
        for(int i = 0; i < savedMaxCategoryNum; i++){
            pros[i] = counts[i] / rowNum;
        }
        return (int) (calculate(pros, jobIDCategoryNum, averageEventNum) * windowIdNum);
    }

    public static int calCrimesDistrict(double estimateEventNum, double windowIdNum){
        double rowNum = 7701079;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] counts = {
                518649, 492918, 451452, 446550, 438386,
                438380, 391229, 380828, 376911, 363931,
                345263, 344821, 340036, 332292, 330714
                //310445, 297793, 256938, 252175, 232518
        };

        int districtCategoryNum = 24;
        double[] pros = new double[savedMaxCategoryNum];
        for(int i = 0; i < savedMaxCategoryNum; i++){
            pros[i] = counts[i] / rowNum;
        }
        return (int) (calculate(pros, districtCategoryNum, averageEventNum) * windowIdNum);
    }

    public static int calCrimesBeat(double estimateEventNum, double windowIdNum){
        double rowNum = 7701079;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] counts = {
                60235, 59706, 54577, 54321, 52788,
                52032, 51765, 51093, 48602, 48071,
                47075, 46886, 46208, 44880, 44224
                //44037, 42757, 42747, 42744, 42683
        };

        int districtCategoryNum = 305;
        double[] pros = new double[savedMaxCategoryNum];
        for(int i = 0; i < savedMaxCategoryNum; i++){
            pros[i] = counts[i] / rowNum;
        }

        //System.out.println("pros: " + Arrays.toString(pros));

        int keyNum = (int) (calculate(pros, districtCategoryNum, averageEventNum) * windowIdNum);
        //System.out.println("keyNum: " + keyNum);
        return keyNum;
    }

    public static int calCitibikeStartStationId(double estimateEventNum, double windowIdNum) {
        double rowNum = 34994041;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] counts = {
                139685, 115377, 110578, 107218, 106955,
                104718, 102620, 102448, 99513, 98189,
                96178, 93799, 93699, 92081, 88999,
                // 88606, 88483, 86803, 86119, 85971
        };

        int districtCategoryNum = 2155;
        double[] pros = new double[savedMaxCategoryNum];
        for(int i = 0; i < savedMaxCategoryNum; i++){
            pros[i] = counts[i] / rowNum;
        }

        return (int) (calculate(pros, districtCategoryNum, averageEventNum) * windowIdNum);
    }

    public static int calCitibikeEndStationId(double estimateEventNum, double windowIdNum) {
        double rowNum = 34994041;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] counts = {
                140302, 112506, 111386, 109602, 107168,
                105392, 102894, 102725, 99946, 99222,
                95920, 94110, 93674, 90897, 89766,
                // 88488, 87393, 87377, 86676, 86381
        };

        int districtCategoryNum = 2155;
        double[] pros = new double[savedMaxCategoryNum];
        for(int i = 0; i < savedMaxCategoryNum; i++){
            pros[i] = counts[i] / rowNum;
        }
        return (int) (calculate(pros, districtCategoryNum, averageEventNum) * windowIdNum);
    }

    public static int calSyntheticA2(double estimateEventNum, double windowIdNum){
        int a3CategoryNum = 50;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] pros = {
                0.04147232049521569, 0.03475327101981663, 0.03133952545807808, 0.029122794002234793, 0.02751192538452083,
                0.026262119140468082, 0.025249822870856798, 0.024404526699457658, 0.02368244275240949, 0.023054639522173015,
                0.022501071586134482, 0.022007317968829137, 0.021562682155293084, 0.021159026718422527, 0.02079002755798054
        };
        if(pros.length != savedMaxCategoryNum){
            throw new RuntimeException("please adjust counts or savedMaxCategoryNum");
        }
        return (int) (calculate(pros, a3CategoryNum, averageEventNum) * windowIdNum);
    }

    public static int calSyntheticA3(double estimateEventNum, double windowIdNum){
        int a4CategoryNum = 50;
        double averageEventNum = (estimateEventNum / windowIdNum);
        double[] pros = {
                0.12449182594511901, 0.07663370800972048, 0.05769736219564887, 0.04717358074503648, 0.040351699790456864,
                0.035516892565955645, 0.031883956567188634, 0.029038745195863483, 0.026740595850874915, 0.024839384882996102,
                0.023236245550072972, 0.02186321193097394, 0.020671904513499487, 0.01962687750151312, 0.01870152213082999
        };
        if(pros.length != savedMaxCategoryNum){
            throw new RuntimeException("please adjust counts or savedMaxCategoryNum");
        }
        int keyNum = (int) (calculate(pros, a4CategoryNum, averageEventNum) * windowIdNum);
        System.out.println("keyNum: " + keyNum);
        return keyNum;
    }

    public static int calKeyNum(String tableName, String attrName, double estimateEventNum, double windowIdNum){
        if(tableName.equals("CRIMES")){
            if(attrName.equals("DISTRICT")){
                return calCrimesDistrict(estimateEventNum, windowIdNum);
            }else if(attrName.equals("BEAT")){
                return calCrimesBeat(estimateEventNum, windowIdNum);
            }else{
                System.out.println("we directly use number of events as number of keys");
                return (int) estimateEventNum;
            }
        }else if(tableName.equals("CITIBIKE")){
            if(attrName.equals("START_STATION_ID")){
                return calCitibikeStartStationId(estimateEventNum, windowIdNum);
            }else if(attrName.equals("END_STATION_ID")){
                return calCitibikeEndStationId(estimateEventNum, windowIdNum);
            }else{
                System.out.println("we directly use number of events as number of keys");
                return (int) estimateEventNum;
            }
        }
        else if(tableName.equals("CLUSTER")){
            if(attrName.equals("JOBID")){
                return calClusterJobId(estimateEventNum, windowIdNum);
            }else{
                System.out.println("we directly use number of events as number of keys");
                return (int) estimateEventNum;
            }
        }
        else if(tableName.equals("SYNTHETIC")){
            if(attrName.equals("A2")){
                return calSyntheticA2(estimateEventNum, windowIdNum);
            }else if(attrName.equals("A3")){
                return calSyntheticA3(estimateEventNum, windowIdNum);
            }else{
                System.out.println("we directly use number of events as number of keys");
                return (int) estimateEventNum;
            }
        }
        return (int) estimateEventNum;
    }
}
