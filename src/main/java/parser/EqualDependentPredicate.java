package parser;

import store.DataType;

/**
 * it can support equal predicate like: v1.beat = v2.beat, v1.beat = v2.beat + 1
 * predicate format we support:
 * leftVarName.leftAttrName leftOperator leftConstValue = rightVarName.rightAttrName rightOperator rightConstValue
 * please note that we will not check exception, so be carefully call related functions
 */
public class EqualDependentPredicate extends DependentPredicate{
    public String leftVarName;
    public String leftAttrName;
    public ArithmeticOperator leftOperator;
    public String leftConstValue;
    // below two code lines can help accelerate obtain value
    public boolean leftHasConverted;
    public double leftValue;

    public String rightVarName;
    public String rightAttrName;
    public ArithmeticOperator rightOperator;
    public String rightConstValue;
    // below two code lines can help accelerate obtain value
    public boolean rightHasConverted;
    public double rightValue;

    public EqualDependentPredicate(String singlePredicate){
        // case 1: v1.beat = v2.beat
        // case 2: v1.beat = v2.beat + 1
        // case 3: v1.beat + 1 = v2.beat + 1
        String[] twoParts = singlePredicate.split("=");

        String leftPart = twoParts[0].trim();
        int leftDotPos = -1;
        for(int i = 0; i < leftPart.length(); ++i){
            char ch = leftPart.charAt(i);
            if(ch == '.'){
                leftDotPos = i;
                leftVarName = leftPart.substring(0,i);
            }
            if(leftDotPos != -1){
                switch (ch){
                    case '*':
                        leftOperator = ArithmeticOperator.MUL;
                        break;
                    case '/':
                        leftOperator = ArithmeticOperator.DIV;
                        break;
                    case '+':
                        leftOperator = ArithmeticOperator.ADD;
                        break;
                    case '-':
                        leftOperator = ArithmeticOperator.SUB;
                        break;
                }
                if(leftOperator != null){
                    leftAttrName = leftPart.substring(leftDotPos + 1, i).trim();
                    leftConstValue =leftPart.substring(i + 1).trim();
                    break;
                }
            }
        }
        if(leftOperator == null){
            leftAttrName = leftPart.substring(leftDotPos + 1);
        }


        String rightPart = twoParts[1].trim();
        int rightDotPos = -1;
        for(int i = 0; i < rightPart.length(); ++i){
            char ch = rightPart.charAt(i);
            if(ch == '.'){
                rightDotPos = i;
                rightVarName = rightPart.substring(0,i);
            }
            if(rightDotPos != -1){
                switch (ch){
                    case '*':
                        rightOperator = ArithmeticOperator.MUL;
                        break;
                    case '/':
                        rightOperator = ArithmeticOperator.DIV;
                        break;
                    case '+':
                        rightOperator = ArithmeticOperator.ADD;
                        break;
                    case '-':
                        rightOperator = ArithmeticOperator.SUB;
                        break;
                }
                if(rightOperator != null){
                    rightAttrName = rightPart.substring(leftDotPos + 1, i).trim();
                    rightConstValue = rightPart.substring(i + 1).trim();
                    break;
                }
            }
        }
        if(rightOperator == null){
            rightAttrName = rightPart.substring(rightDotPos + 1);
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

        switch (dataType) {
            case VARCHAR:
                // if data type is string, we directly compare
                // because we cannot support "XXX" + "YYY" this expression
                String str1 = (String) o1;
                String str2 = (String) o2;
                return str1.equals(str2);
            case INT:
                return (int) getLeftValue(o1, dataType) == (int) getRightValue(o2, dataType);
            case LONG:
                return (long) getLeftValue(o1, dataType) == (long) getRightValue(o2, dataType);
            case FLOAT:
                return (float) getLeftValue(o1, dataType) == (float) getRightValue(o2, dataType);
            default:
                // DOUBLE:
                return (double) getLeftValue(o1, dataType) == (double) getRightValue(o2, dataType);
        }

    }

    @Override
    public boolean isEqualDependentPredicate() {
        return true;
    }

    @Override
    public boolean alignedCheck(Object o1, Object o2, DataType dataType) {
        if(dataType == DataType.VARCHAR){
            String str1 = (String) o1;
            String str2 = (String) o2;
            return str1.equals(str2);
        }else{
            switch (dataType) {
                case INT:
                    return (int) getLeftValue(o1, dataType) == (int) getRightValue(o2, dataType);
                case LONG:
                    return (long) getLeftValue(o1, dataType) == (long) getRightValue(o2, dataType);
                case FLOAT:
                    return (float) getLeftValue(o1, dataType) == (float) getRightValue(o2, dataType);
                default:
                    // DOUBLE:
                    return (double) getLeftValue(o1, dataType) == (double) getRightValue(o2, dataType);
            }
        }
    }

    @Override
    public Object getOneSideValue(String varName, Object value, DataType dataType) {
        return varName.equals(leftVarName) ? getLeftValue(value, dataType) : getRightValue(value, dataType);
    }

    /**
     * after obtaining the return result, we can use variableName.getClass().cast(obj)
     * @param value     -   number value
     * @param dataType  -   data type
     * @return          -   data value
     */
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

        // Object should be: STRING, INT, LONG, FLOAT, DOUBLE
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
        System.out.print(" = " + rightVarName + "." + rightAttrName);
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
    public String toString(){
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
        sb.append(" = ").append(rightVarName).append(".").append(rightAttrName);
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
}
