package ultiliities.validation;

public class Validation {
    public static int validateOpe(double b, String ope) {
        if (!ope.equalsIgnoreCase("+") && !ope.equalsIgnoreCase("-") && !ope.equalsIgnoreCase("x")
                && !ope.equalsIgnoreCase(":")) {
            return -1;
        }
        if((ope.equalsIgnoreCase(":") && b == 0)){
            return 0;

        }
        return 1;
    }
    public static boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            double d = Double.parseDouble(strNum);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

}
