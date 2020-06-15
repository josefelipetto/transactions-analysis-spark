public class Utils {

    public static Long parseLong(String value) {
        return value.equals("") ? 0 : Double.valueOf(value).longValue();
    }
}
