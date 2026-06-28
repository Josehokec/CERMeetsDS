package plan;

import java.util.ArrayList;
import java.util.List;

/**
 * plan format: [variableName1, ..., variableName_n]
 */
public class Plan {
    private List<String> steps;

    public Plan(int len){
        steps = new ArrayList<>(len);
    }

    public Plan(List<String> steps) {
        this.steps = steps;
    }

    public List<String> getSteps() {
        return steps;
    }

    public int length(){
        return steps.size();
    }

    public void add(String step){
        steps.add(step);
    }

//    public String getVariableName(int pos){
//        return plan.get(pos);
//    }

    @Override
    public String toString(){
        return steps.toString();
    }

    public boolean hasVarName(String varName){
        for(String step : steps){
            if(step.equals(varName)){
                return true;
            }
        }
        return false;
    }

    public String getLastVarName(){
        return steps.get(steps.size()-1);
    }

    // get all plans
    public Plan copy(){
        List<String> newSteps = new ArrayList<>(steps);
        return new Plan(newSteps);
    }
}
