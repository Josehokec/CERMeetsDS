package engine;

import parser.DependentPredicate;
import parser.IndependentPredicate;

import store.ColumnInfo;
import store.EventSchema;
import utils.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class Transition {
    private final List<Pair<IndependentPredicate, ColumnInfo>> ipPairs;     //  independent constraint list
    private final List<DependentPredicate> dpList;                          // dependent constraint list
    private final State nextState;                                          // next state

    public Transition(List<Pair<IndependentPredicate, ColumnInfo>> ipPairs, List<DependentPredicate> dpList, State nextState){
        this.ipPairs = ipPairs;
        this.dpList = dpList;
        this.nextState = nextState;
    }

    public boolean checkIndependentPredicate(byte[] record, EventSchema schema){
        for(Pair<IndependentPredicate, ColumnInfo> ipPair : ipPairs){
            IndependentPredicate ip = ipPair.getKey();
            ColumnInfo columnInfo = ipPair.getValue();
            ByteBuffer bb = ByteBuffer.wrap(record);
            Object obj = schema.getColumnValue(columnInfo, bb, 0);
            boolean satisfy = ip.check(obj, columnInfo.getDataType());
            if(!satisfy){
                return false;
            }
        }
        return true;
    }

    public List<DependentPredicate> getDependentPredicateList(){
        return dpList;
    }

    public State getNextState(){
        return nextState;
    }

    public void print(){
        List<IndependentPredicate> ipList = new ArrayList<>(ipPairs.size());
        for(Pair<IndependentPredicate, ColumnInfo> ipPair : ipPairs){
            ipList.add(ipPair.getKey());
        }

        System.out.println("  --transition information");
        System.out.println("  ----independent predicate conditions: " + ipList);
        System.out.println("  ----dependent predicate conditions: " + dpList);
        System.out.println("  ----next state information: " + nextState);
        //System.out.println("\n");
    }
}
