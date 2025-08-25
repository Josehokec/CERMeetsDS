package engine;


import parser.DependentPredicate;
import store.EventSchema;

import java.util.*;

public class State {
    private final String stateName;                 // state name (variable name)
    private final int stateId;                      // stateId can unique represent a state
    private final List<Transition> transitions;     // edges / transactions
    private PartialMatchCache partialMatchCache;    // partial match buffer
    private boolean isFinal;                        // final state
    private boolean isStart;                        // start state

    public State(String stateName, int stateId){
        this.stateName = stateName;
        this.stateId = stateId;
        transitions = new ArrayList<>();
        partialMatchCache = null;
        isFinal = false;
        isStart = false;
    }

    public boolean getIsFinal(){
        return isFinal;
    }

    public void setFinal() { isFinal = true; }

    public void setStart(){
        isStart = true;
    }

    public String getStateName(){
        return stateName;
    }

    public List<Transition> getTransactions() { return transitions; }

    public void bindTransaction(Transition transition){
        transitions.add(transition);
    }

    public void bindBuffer(PartialMatchCache partialMatchCache){
        this.partialMatchCache = partialMatchCache;
    }

    /**
     * @param eventCache - NFA's event cache
     * @param record - byte record (please keep it arrival in order)
     * @param window - query window
     * @param strategy - selection strategy (currently, we only support SKIP_TILL_NEXT_MATCH and SKIP_TILL_ANY_MATCH)
     * @return - next state
     */
    public Set<State> transition(EventCache eventCache, byte[] record, long window, SelectionStrategy strategy, EventSchema schema){
        Set<State> nextStates = new HashSet<>();
        long timestamp = schema.getTimestamp(record);
        // for each transition (a state can transition multiple state)
        for(Transition transition : transitions){
            // first check independent constraints
            if(transition.checkIndependentPredicate(record, schema)){
                State nextState = transition.getNextState();
                boolean hasTransition = false;
                String partitionName = schema.getColumnValue(NFA.partitionColName, record).toString();

                // here we need to judge whether current state whether is start state
                // if yes, then we directly add this record to next state's match buffer
                // otherwise, we need to check dependent predicates
                if(isStart){
                    int recordPointer = eventCache.insert(record);
                    // this state is start state, we need to generate a partial match
                    List<Integer> eventPointers = new ArrayList<>(4);
                    eventPointers.add(recordPointer);
                    // generate a partial match, start time and end time is record's timestamp
                    PartialMatch match = new PartialMatch(timestamp, timestamp, eventPointers);
                    // add the partial match to match buffer
                    PartialMatchCache nextMatchCache =  nextState.getPartialMatchCache();

                    if(nextMatchCache == null){
                        // create a buffer and bind to a state
                        List<String> stateNames = new ArrayList<>();
                        stateNames.add(nextState.stateName);
                        nextMatchCache = new PartialMatchCache(stateNames, partitionName);
                        nextState.bindBuffer(nextMatchCache);
                    }

                    nextMatchCache.addPartialMatch(match, partitionName);
                    hasTransition = true;
                }
                else{
                    int hasInsertedPointers = -1;
                    List<PartialMatch> curPartialMatches = partialMatchCache.getPartialMatchList(partitionName);

                    Set<Integer> headerSet = new HashSet<>();
                    Iterator<PartialMatch> it = curPartialMatches.iterator();
                    while(it.hasNext()){
                        PartialMatch curMatch = it.next();
                        if(headerSet.contains(curMatch.getPointer(0))){
                            it.remove();
                            continue;
                        }

                        long matchStartTime = curMatch.getStartTime();
                        boolean timeout = timestamp - matchStartTime > window;
                        // if timeout we need to remove this partial match
                        if(timeout){
                            it.remove();
                        }
                        // please note that timestamp is non-decreasing
                        else{
                            boolean satisfyAllDP = true;
                            for(DependentPredicate dp : transition.getDependentPredicateList()){
                                // stateName ==> variable name
                                String nextVarName = nextState.getStateName();

                                String nextAttrName = dp.getAttrName(nextVarName);
                                Object nextObj = schema.getColumnValue(nextAttrName, record);
                                boolean nextIsLeft = dp.getLeftVarName().equals(nextVarName);

                                String anotherVarName = nextIsLeft ? dp.getRightVarName() : dp.getLeftVarName();
                                String anotherAttrName = nextIsLeft ? dp.getRightAttrName() : dp.getLeftAttrName();
                                int pos = partialMatchCache.findStateNamePosition(anotherVarName);
                                byte[] findRecord = eventCache.get(curMatch.getPointer(pos));
                                Object anotherObj = schema.getColumnValue(anotherAttrName, findRecord);
                                // we please note that we require same types
                                satisfyAllDP = dp.check(nextVarName, nextObj, anotherVarName, anotherObj, schema.getDataType(nextAttrName));
                                if(!satisfyAllDP) {
                                    break;
                                }
                            }

                            if(satisfyAllDP){
                                //wrong code: int recordPointer = eventCache.insert(record);
                                hasInsertedPointers = hasInsertedPointers == -1 ? eventCache.insert(record) : hasInsertedPointers;

                                // selection strategy
                                if(strategy == SelectionStrategy.SKIP_TILL_NEXT_MATCH){
                                    // once can match then this partial match cannot match others events
                                    it.remove();
                                }
                                // [2025-4-8]
                                else if(strategy == SelectionStrategy.AFTER_MATCH_SKIP_TO_NEXT_ROW){
                                    if(NFA.withoutDC){
                                        it.remove();
                                    }
                                    else if(nextState.isFinal){
                                        headerSet.add(curMatch.getPointer(0));
                                        it.remove();
                                    }
                                }

                                // create a match and add it to next buffer
                                List<Integer> newEventPointers = new ArrayList<>(curMatch.getRecordPointers());
                                newEventPointers.add(hasInsertedPointers);
                                PartialMatch match = new PartialMatch(curMatch.getStartTime(), timestamp, newEventPointers);

                                PartialMatchCache nextCache = nextState.getPartialMatchCache();

                                if(nextCache == null){
                                    // create a buffer and bind to a state
                                    List<String> stateNames = new ArrayList<>(partialMatchCache.getStateNames());
                                    stateNames.add(nextState.getStateName());
                                    nextCache = new PartialMatchCache(stateNames, partitionName);
                                    nextState.bindBuffer(nextCache);
                                }
                                nextCache.addPartialMatch(match, partitionName);
                                hasTransition = true;
                            }
                        }
                    }
                }

                if(hasTransition){
                    nextStates.add(nextState);
                }
            }
        }

        return nextStates;
    }

    public PartialMatchCache getPartialMatchCache(){
        return partialMatchCache;
    }

    public String toString(){
        return " [stateId: " + stateId +
                ", stateName: " + stateName +
                ", isStart: " + isStart +
                ", isFinal: " + isFinal +
                ", transitionNum: " + transitions.size() + "]";
    }

    public void recursiveDisplayState(){
        System.out.println("$ state information: " + this);
        //System.out.println(this);
        if(!isFinal){
            for(Transition t : transitions){
                t.print();
                State nextState = t.getNextState();
                nextState.recursiveDisplayState();
            }
        }
    }
}
