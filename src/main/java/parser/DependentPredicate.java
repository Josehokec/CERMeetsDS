package parser;

import store.DataType;

/**
 * regex format of equal predicate:
 * varName1.attrName [[+-x/] C1]? = varName1.attrName [[+-x/] C2]?
 */
public abstract class DependentPredicate {
    public abstract String getLeftVarName();
    public abstract String getLeftAttrName();
    public abstract String getRightVarName();
    public abstract String getRightAttrName();
    public abstract String getAttrName(String varName);
    // because we want to implement a generic check function, so we use Object as argument
    public abstract boolean check(String varName1, Object o1, String varName2, Object o2, DataType dataType);
    public abstract boolean isEqualDependentPredicate();
    // call this function rather than "check" function will have a better performance
    public abstract boolean alignedCheck(Object o1, Object o2, DataType dataType);
    public abstract Object getOneSideValue(String variableName, Object value, DataType dataType);
    public abstract void print();
}
