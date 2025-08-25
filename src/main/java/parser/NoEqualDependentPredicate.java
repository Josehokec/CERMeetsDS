package parser;

import store.DataType;

/**
 * support format:
 * leftVarName.attrName leftOperator leftConstValue [><][=]?|[!=] rightVarName.attrName rightOperator rightConstValue
 */

public class NoEqualDependentPredicate extends DependentPredicate {
    public String leftVarName;
    public String leftAttrName;
    public ArithmeticOperator leftOperator;
    public String leftConstValue;
    public boolean leftHasConverted;
    public double leftValue;

    public ComparedOperator cmp;           // '>' | '>=' | '<' | '<=' | '!='

    public String rightVarName;
    public String rightAttrName;
    public ArithmeticOperator rightOperator;
    public String rightConstValue;
    public boolean rightHasConverted;
    public double rightValue;

    public NoEqualDependentPredicate(String singlePredicate) {
        int leftDotPos = -1;
        int rightDotPos = -1;

        // we use it to mark whether we finish left part decoding
        int finishLeftPart = -1;
        int leftOperatorPos = -1;

        leftOperator = null;
        rightOperator = null;

        String str = singlePredicate.trim();
        int len = str.length();
        for (int i = 0; i < len; ++i) {
            char ch = str.charAt(i);

            if (ch == '.') {
                // if '.' is the first dot
                if (leftDotPos == -1) {
                    leftDotPos = i;
                    leftVarName = str.substring(0, i);
                } else {
                    if(finishLeftPart != -1){
                        rightDotPos = i;
                        rightVarName = str.substring(finishLeftPart, i).trim();
                    }
                }

            }
            // '+' | '-' | '*' | '/'
            if (ch == '+' || ch == '-' || ch == '*' || ch == '/') {
                ArithmeticOperator curArithmeticOperator;
                switch (ch) {
                    case '+':
                        curArithmeticOperator = ArithmeticOperator.ADD;
                        break;
                    case '-':
                        curArithmeticOperator = ArithmeticOperator.SUB;
                        break;
                    case '*':
                        curArithmeticOperator = ArithmeticOperator.MUL;
                        break;
                    default:
                        // -> '/'
                        curArithmeticOperator = ArithmeticOperator.DIV;
                }
                if (finishLeftPart == -1) {
                    leftOperatorPos = i;
                    leftAttrName = str.substring(leftDotPos + 1, i).trim();
                    leftOperator = curArithmeticOperator;
                } else {
                    rightAttrName = str.substring(rightDotPos + 1, i).trim();
                    rightOperator = curArithmeticOperator;
                    rightConstValue = str.substring(i + 1).trim();
                    break;
                }
            }

            // '>' | '>=' | '<' | <=' | '!='
            if (ch == '>' || ch == '<' || ch == '!') {
                if (leftOperator == null) {
                    leftConstValue = null;
                    String temp = str.substring(leftDotPos + 1, i);
                    leftAttrName = temp.trim();
                } else {
                    leftConstValue = str.substring(leftOperatorPos + 1, i).trim();
                }
                char nextChar = str.charAt(i + 1);
                if (nextChar == '=') {
                    switch (ch) {
                        case '>':
                            cmp = ComparedOperator.GE;
                            break;
                        case '<':
                            cmp = ComparedOperator.LE;
                            break;
                        default:
                            // '!'
                            cmp = ComparedOperator.NEQ;
                    }
                    finishLeftPart = i + 2;
                } else {
                    switch (ch) {
                        case '>':
                            cmp = ComparedOperator.GT;
                            break;
                        case '<':
                            cmp = ComparedOperator.LT;
                    }
                    finishLeftPart = i + 1;
                }
            }
        }
        // solve case 4: v1.beat + 1 > v2.beat
        if (rightOperator == null) {
            rightAttrName = str.substring(rightDotPos + 1);
        }

    }

    @Override
    public String getLeftVarName() {
        return leftVarName;
    }

    @Override
    public String getLeftAttrName() {
        return leftAttrName;
    }

    @Override
    public String getRightVarName() {
        return rightVarName;
    }

    @Override
    public String getRightAttrName() {
        return rightAttrName;
    }

    @Override
    public String getAttrName(String varName) {
        return varName.equals(leftVarName) ? leftAttrName : rightAttrName;
    }

    @Override
    public boolean check(String varName1, Object o1, String varName2, Object o2, DataType dataType) {
        // before calling this function, you should ensure expression is correct
        boolean isAlign = varName1.equalsIgnoreCase(leftVarName);
        if(!isAlign){
            // below three lines only used for debugging
            if(!varName2.equalsIgnoreCase(leftVarName) || !varName1.equals(rightVarName)){
                throw new RuntimeException("mismatched variable name");
            }
            // convert two objects
            Object o = o1;
            o1 = o2;
            o2 = o;
        }else{
            // below three lines only used for debugging
            if(!varName2.equalsIgnoreCase(rightVarName)){
                throw new RuntimeException("mismatched variable name");
            }
        }

        int comparedResult;
        switch (dataType) {
            case VARCHAR:
                // if data type is string, we directly compare
                // because we cannot support "XXX" + "YYY" this expression
                String str1 = (String) o1;
                String str2 = (String) o2;
                comparedResult = str1.compareTo(str2);
                break;
            case INT:
                Integer intValue =  (int) getLeftValue(o1, dataType);
                comparedResult = intValue.compareTo((int) getRightValue(o2, dataType));
                break;
            case LONG:
                Long longValue = (long) getLeftValue(o1, dataType);
                comparedResult = longValue.compareTo((long) getRightValue(o2, dataType));
                break;
            case FLOAT:
                Float floatValue = (float) getLeftValue(o1, dataType);
                comparedResult = floatValue.compareTo((float) getRightValue(o2, dataType));
                break;
            default:
                // DOUBLE:
                Double doubleValue =  (double) getLeftValue(o1, dataType);
                comparedResult = doubleValue.compareTo((double) getRightValue(o2, dataType));
        }
        switch (cmp){
            case GT:
                return comparedResult > 0;
            case GE:
                return comparedResult >= 0;
            case LT:
                return comparedResult < 0;
            case LE:
                return comparedResult <= 0;
            default:
                // NEQ
                return comparedResult != 0;
        }
    }

    @Override
    public boolean isEqualDependentPredicate() {
        return false;
    }

    @Override
    public Object getOneSideValue(String varName, Object value, DataType dataType) {
        return varName.equals(leftVarName) ? getLeftValue(value, dataType) : getRightValue(value, dataType);
    }

    @Override
    public boolean alignedCheck(Object o1, Object o2, DataType dataType) {
        int comparedResult;
        switch (dataType) {
            case VARCHAR:
                // if data type is string, we directly compare
                // because we cannot support "XXX" + "YYY" this expression
                String str1 = (String) o1;
                String str2 = (String) o2;
                comparedResult = str1.compareTo(str2);
                break;
            case INT:
                Integer intValue =  (int) getLeftValue(o1, dataType);
                comparedResult = intValue.compareTo((int) getRightValue(o2, dataType));
                break;
            case LONG:
                Long longValue = (long) getLeftValue(o1, dataType);
                comparedResult = longValue.compareTo((long) getRightValue(o2, dataType));
                break;
            case FLOAT:
                Float floatValue = (float) getLeftValue(o1, dataType);
                comparedResult = floatValue.compareTo((float) getRightValue(o2, dataType));
                break;
            default:
                // DOUBLE:
                Double doubleValue =  (double) getLeftValue(o1, dataType);
                comparedResult = doubleValue.compareTo((double) getRightValue(o2, dataType));
        }
        switch (cmp){
            case GT:
                return comparedResult > 0;
            case GE:
                return comparedResult >= 0;
            case LT:
                return comparedResult < 0;
            case LE:
                return comparedResult <= 0;
            default:
                // NEQ
                return comparedResult != 0;
        }
    }

    public final Object getLeftValue(Object value, DataType dataType){
        // Object should be: STRING, INT, LONG, FLOAT, DOUBLE
        if(dataType == DataType.VARCHAR || leftOperator == null){
            // we cannot support string +-x/
            return value;
        }
        // we use leftValue to accelerate get value
        if(!leftHasConverted){
            leftValue = Double.parseDouble(leftConstValue);
            leftHasConverted = true;
        }

        switch (dataType) {
            case INT:
                switch (leftOperator) {
                    case ADD:
                        return (int) ((int) value + leftValue);
                    case SUB:
                        return (int) ((int) value - leftValue);
                    case MUL:
                        return (int) ((int) value * leftValue);
                    default:
                        // DIV
                        return (int) ((int) value / leftValue);
                }
            case LONG:
                switch (leftOperator) {
                    case ADD:
                        return (long) ((long) value + leftValue);
                    case SUB:
                        return (long) ((long) value - leftValue);
                    case MUL:
                        return (long) ((long) value * leftValue);
                    default:
                        // DIV
                        return (long) ((long) value / leftValue);
                }
            case FLOAT:
                switch (leftOperator) {
                    case ADD:
                        return (float) ((float) value + leftValue);
                    case SUB:
                        return (float) ((float) value - leftValue);
                    case MUL:
                        return (float) ((float) value * leftValue);
                    default:
                        // DIV
                        return (float) ((float) value / leftValue);
                }
            default:
                // DOUBLE
                switch (leftOperator) {
                    case ADD:
                        return (double) value + leftValue;
                    case SUB:
                        return (double) value - leftValue;
                    case MUL:
                        return (double) value * leftValue;
                    default:
                        // DIV
                        return (double) value / leftValue;
                }
        }
    }

    public final Object getRightValue(Object value, DataType dataType){
        // Object should be: STRING, INT, LONG, FLOAT, DOUBLE
        if(dataType == DataType.VARCHAR || rightOperator == null){
            // we cannot support string +-x/
            return value;
        }
        // we use leftValue to accelerate get value
        if(!rightHasConverted){
            rightValue = Double.parseDouble(rightConstValue);
            rightHasConverted = true;
        }

        // Object should be: STRING, INT, LONG, FLOAT, DOUBLE
        switch (dataType) {
            case INT:
                switch (rightOperator) {
                    case ADD:
                        return (int) ((int) value + rightValue);
                    case SUB:
                        return (int) ((int) value - rightValue);
                    case MUL:
                        return (int) ((int) value * rightValue);
                    default:
                        // DIV
                        return (int) ((int) value / rightValue);
                }
            case LONG:
                switch (rightOperator) {
                    case ADD:
                        return (long) ((long) value + rightValue);
                    case SUB:
                        return (long) ((long) value - rightValue);
                    case MUL:
                        return (long) ((long) value * rightValue);
                    default:
                        // DIV
                        return (long) ((long) value / rightValue);
                }
            case FLOAT:
                switch (rightOperator) {
                    case ADD:
                        return (float) ((float) value + rightValue);
                    case SUB:
                        return (float) ((float) value - rightValue);
                    case MUL:
                        return (float) ((float) value * rightValue);
                    default:
                        // DIV
                        return (float) ((float) value / rightValue);
                }
            default:
                switch (rightOperator) {
                    case ADD:
                        return (double) value + rightValue;
                    case SUB:
                        return (double) value - rightValue;
                    case MUL:
                        return (double) value * rightValue;
                    default:
                        // DIV
                        return (double) value / rightValue;
                }
        }
    }

    @Override
    public void print() {
        System.out.print(leftVarName + "." + leftAttrName);
        if(leftOperator != null){
            switch (leftOperator){
                case ADD:
                    System.out.print(" + " + leftConstValue);
                    break;
                case SUB:
                    System.out.print(" - " + leftConstValue);
                    break;
                case MUL:
                    System.out.print(" * " + leftConstValue);
                    break;
                default:
                    // DIV
                    System.out.print(" / " + leftConstValue);
            }
        }
        // compared operator
        switch (cmp){
            case GT:
                System.out.print(" > ");
                break;
            case GE:
                System.out.print(" >= ");
                break;
            case LT:
                System.out.print(" < ");
                break;
            case LE:
                System.out.print(" <= ");
                break;
            case NEQ:
                System.out.print(" != ");
                break;
            default:
                throw new RuntimeException("this compared operator is illegal");
        }

        System.out.print(rightVarName + "." + rightAttrName);
        if(rightOperator != null){
            switch (rightOperator){
                case ADD:
                    System.out.print(" + " + rightConstValue);
                    break;
                case SUB:
                    System.out.print(" - " + rightConstValue);
                    break;
                case MUL:
                    System.out.print(" * " + rightConstValue);
                    break;
                default:
                    // DIV
                    System.out.print(" / " + rightConstValue);
            }
        }
        System.out.println();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(leftVarName).append(".").append(leftAttrName);
        if(leftOperator != null){
            switch (leftOperator){
                case ADD:
                    sb.append(" + ").append(leftConstValue);
                    break;
                case SUB:
                    sb.append(" - ").append(leftConstValue);
                    break;
                case MUL:
                    sb.append(" * ").append(leftConstValue);
                    break;
                default:
                    // DIV
                    sb.append(" / ").append(leftConstValue);
            }
        }
        // compared operator
        switch (cmp){
            case GT:
                sb.append(" > ");
                break;
            case GE:
                sb.append(" >= ");
                break;
            case LT:
                sb.append(" < ");
                break;
            case LE:
                sb.append(" <= ");
                break;
            case NEQ:
                sb.append(" != ");
                break;
            default:
                throw new RuntimeException("this compared operator is illegal");
        }

        sb.append(rightVarName).append(".").append(rightAttrName);
        if(rightOperator != null){
            switch (rightOperator){
                case ADD:
                    sb.append(" + ").append(rightConstValue);
                    break;
                case SUB:
                    sb.append(" - ").append(rightConstValue);
                    break;
                case MUL:
                    sb.append(" * ").append(rightConstValue);
                    break;
                default:
                    // DIV
                    sb.append(" / ").append(rightConstValue);
            }
        }
        return sb.toString();
    }

    public ArithmeticOperator getLeftOperator() {
        return leftOperator;
    }

    public String getLeftConstValue() {
        return leftConstValue;
    }

    public ComparedOperator getCmp() {
        return cmp;
    }

    public ArithmeticOperator getRightOperator() {
        return rightOperator;
    }

    public String getRightConstValue() {
        return rightConstValue;
    }
}
/*
    public static void main(String[] args){
        // case 1: v1.beat > v2.beat
        // case 2: v1.beat != v2.beat + 1
        // case 3: v1.beat + 1 <= v2.beat * 1
        // case 4: v1.beat + 1 > v2.beat

        String predicate1 = "v1.beat > v2.beat";
        NoEqualDependentPredicate nedp1 = new NoEqualDependentPredicate(predicate1);
        nedp1.print();
        System.out.println("true: " + nedp1.check("v1", 10, "v2", 9, DataType.INT));
        System.out.println("false: " + nedp1.check("v1", 10, "v2", 10, DataType.INT));
        System.out.println("true: " + nedp1.check("v2", 10, "v1", 11, DataType.INT));


        String predicate2 = "v1.beat != v2.beat + 1";
        NoEqualDependentPredicate nedp2 = new NoEqualDependentPredicate(predicate2);
        nedp2.print();
        System.out.println("false: " + nedp2.check("v1", 6f, "v2", 5f, DataType.FLOAT));
        System.out.println("false: " + nedp2.check("v2", 6f, "v1", 7f, DataType.FLOAT));
        System.out.println("true: " + nedp2.check("v1", 6f, "v2", 6f, DataType.FLOAT));


        String predicate3 = "v1.beat + 1 <= v2.beat * 2.5";
        NoEqualDependentPredicate nedp3 = new NoEqualDependentPredicate(predicate3);
        nedp3.print();
        System.out.println("true: " + nedp3.check("v1", 9.0, "v2", 4.0, DataType.DOUBLE));
        System.out.println("true: " + nedp3.check("v2", 2.0, "v1", 0.0, DataType.DOUBLE));
        System.out.println("true: " + nedp3.check("v1", -1.0, "v2", 1.0, DataType.DOUBLE));

        String predicate4 = "v1.beat + 1 > v2.beat";
        NoEqualDependentPredicate nedp4 = new NoEqualDependentPredicate(predicate4);
        nedp4.print();
        System.out.println("true: " + nedp3.check("v1", 9.0, "v2", 9.0, DataType.DOUBLE));
        System.out.println("true: " + nedp3.check("v2", 2.0, "v1", 0.0, DataType.DOUBLE));

        String predicate5 = "v1.str != v2.str";
        NoEqualDependentPredicate edp5 = new NoEqualDependentPredicate(predicate5);
        edp5.print();
        System.out.println("false: " + edp5.check("v1", "XXX", "v2", "XXX", DataType.VARCHAR));
        System.out.println("true: " + edp5.check("v1", "XXX", "v2", "YYY", DataType.VARCHAR));
    }
 */