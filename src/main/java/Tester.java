import java.util.Objects;
import java.util.Random;

public class Tester {
    private static int num;

    public static void main(String[] args) {
        if (OnThrowRerunService.getInstance(null, new Object[]{args}).runCurrentMethod()) return;

        System.out.println("main");
        test1();
        test2("test2");
        test3("test3", null);
        Tester tester = new Tester();
        System.out.println(tester.test4() + tester.test4());
    }

    static int c = 0;
    private static void test1() {
        if (OnThrowRerunService.simpleRunCurrentMethod(null).isSuccess()) return;

        if(c++ < 2) {
            test1();
        }
        if (num < 1) {
            num = 1;
            throw new RuntimeException("run test1 throw");
        }
        System.out.println("test1"+c);
        num = 0;

    }

    private static void test2(String arg) {
        if (OnThrowRerunService.simpleRunCurrentMethod(null, arg).isSuccess()) return;

        if (num < 2) {
            num = 2;
            throw new RuntimeException("run test2 throw");
        }
        System.out.println("test2:" + arg);
    }

    private static void test3(String arg1, String arg2) {
        if (OnThrowRerunService.getInstance(null, arg1, arg2)
                .setThrowHandler((service, t) -> service.updateArguments(String.valueOf(num = 3), "3"))
                .setRerunCountLimit(3)
                .runCurrentMethod()) return;

        if (!Objects.equals(arg1, arg2)) {
            throw new RuntimeException("run test3 throw");
        }
        System.out.println("test3:" + arg1 + "," + arg2);
    }

    public String test4() {
        OnThrowRerunService service = OnThrowRerunService.simpleRunCurrentMethod(this);
        if (service.isSuccess()) {
            return service.getResult(String.class);
        }

        if (num < 4) {
            num = 4;
            throw new RuntimeException("run test4 throw");
        }
        return "test4";
    }
}