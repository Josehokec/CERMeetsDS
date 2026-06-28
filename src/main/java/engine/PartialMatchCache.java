package engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PartialMatchCache {
    private final List<String> stateNames;
    private final HashMap<String, List<PartialMatch>> partialMatches;

    public double getPartialMatchCacheSize(){
        double ans = 0;
        int len = stateNames.size();
        for(List<PartialMatch> value : partialMatches.values()){
            ans += len * 4 * value.size();
        }
        return ans;
    }

    public PartialMatchCache(List<String> stateNames, String partitionName){
        // this.length = length;
        this.stateNames = stateNames;
        if(!partitionName.equals("null")){
            partialMatches = new HashMap<>(4);
            List<PartialMatch> partialMatchList = new ArrayList<>(256);
            partialMatches.put("null", partialMatchList);
        }else{
            partialMatches = new HashMap<>(32);
        }
    }

    public List<PartialMatch> getPartialMatchList(String partitionName){
        return partialMatches.getOrDefault(partitionName, new ArrayList<>());
    }

    public List<PartialMatch> getMatchList(){
        List<PartialMatch> matches = new ArrayList<>(512);
        for(List<PartialMatch> partialMatchList : partialMatches.values()){
            matches.addAll(partialMatchList);
        }
        return matches;
    }

    public int getMatchCount(){
        int count = 0;
        for(List<PartialMatch> partialMatchList : partialMatches.values()){
            count += partialMatchList.size();
        }
        return count;
    }

    public int findStateNamePosition(String stateName){
        // below code is slow
        for(int i = 0; i < stateNames.size(); ++i){
            if(stateNames.get(i).equals(stateName)){
                return i;
            }
        }
        throw new RuntimeException("cannot find stateName: " + stateName);
    }

    public void addPartialMatch(PartialMatch match, String partitionName){
        partialMatches.computeIfAbsent(partitionName, k -> new ArrayList<>()).add(match);
    }

    public List<String> getStateNames(){
        return stateNames;
    }

    public int getPartialMatchSize(){
        int sumCount = 0;
        for(List<PartialMatch> partialMatchList : partialMatches.values()){

            sumCount += partialMatchList.size();

        }
        return sumCount;
    }
}
