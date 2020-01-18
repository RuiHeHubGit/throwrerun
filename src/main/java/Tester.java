import java.io.Serializable;
import java.util.Objects;

public class Tester {
    private static int num;

    public static void main(String[] args) {
        if (Retry.getInstance(null, new Object[]{args}).runCurrentMethod()) return;

        System.out.println("main");
        test1();
        test2("test2");
        test3("test3", null);
        Tester tester = new Tester();
        System.out.println(tester.test4() + tester.test4());
        test5();
        test6();
        test7();
    }

    static int c = 0;

    private static void test1() {
        if (Retry.simpleRunCurrentMethod(null).isSuccess()) return;

        if (c++ < 2) {
            test1();
        }
        if (num < 1) {
            num = 1;
            throw new RuntimeException("run test1 throw");
        }
        System.out.println("test1" + c);
        num = 0;

    }

    private static void test2(String arg) {
        if (Retry.simpleRunCurrentMethod(null, arg).isSuccess()) return;
        if (num < 2) {
            num = 2;
            throw new RuntimeException("run test2 throw");
        }
        System.out.println("test2:" + arg);
    }

    private static void test3(String arg1, String arg2) {
        if (Retry.getInstance(null, arg1, arg2)
                .setThrowHandler((service, t) -> service.updateArguments(String.valueOf(num = 3), "3"))
                .setRetryTotal(1)
                .runCurrentMethod()) return;

        if (!Objects.equals(arg1, arg2)) {
            throw new RuntimeException("run test3 throw");
        }
        System.out.println("test3:" + arg1 + "," + arg2);
    }

    private static void test3(String arg1, Serializable arg2) {
        System.out.println("test3 overloaded1");
    }

    private static void test3(String arg1, Object arg2) {
        System.out.println("test3 overloaded2");
    }

    private static void test3(Long arg1, String arg2) {
        System.out.println("test3 overloaded3");
    }

    public String test4() {
        Retry service = Retry.simpleRunCurrentMethod(this);
        if (service.isSuccess()) {
            return (String) service.getResult();
        }

        if (num < 4) {
            num = 4;
            throw new RuntimeException("run test4 throw");
        }
        return "test4";
    }

    public static void test5() {
        String result = Retry.run(() -> getString("test5"));
        System.out.println(result);
    }

    public static void test6() {
        String result = Retry.run(() -> getString("test6"), 5);
        System.out.println(result);
    }

    public static void test7() {
        String result = Retry.run(() -> getString("test7"), (service, t) -> System.out.println(t.getMessage()));
        System.out.println(result);
    }

    private static String getString(String str) {
        if (Math.random() < 0.5) {
            throw new RuntimeException("run getString throw");
        }
        return str;
    }

}