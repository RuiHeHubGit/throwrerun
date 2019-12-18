import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class Retry {
    private static ThreadLocal<Map<String, Retry>> rerunMap;
    private static boolean errorLog;
    private static boolean init;
    private static int defaultRunTotalLimit;
    private ThrowHandler throwHandler;
    private int rerunTotalLimit;
    private Method method;
    private Object target;
    private Object[] arguments;
    private Object result;
    private String key;
    private String className;
    private Integer calledLine;
    private String description;
    private boolean success;
    private boolean runnable;
    private boolean running;
    private boolean hasNext;

    // Lazy loading
    private static class STATIC_FINAL_HOLDER {
        private static final Logger logger = LoggerFactory.getLogger(Retry.class);
        private static final String METHOD_NAME_KEY1 = "getInstance";
        private static final String METHOD_NAME_KEY2 = "simpleRunCurrentMethod";
        private static final String PACKAGE_JAVA_LANG = "java.lang";
        private static final String PACKAGE_SUN_REFLECT = "sun.reflect";
        private static final String CURRENT_CLASS_NAME = Retry.class.getName();
        private static final Retry INVALID_RETRY;
        public static final boolean init;

        static {
            errorLog = Boolean.parseBoolean(System.getProperty("Retry.log", "true"));
            defaultRunTotalLimit = Integer.getInteger("Retry.defaultRerunTotalLimit", 3);
            INVALID_RETRY = new Retry();
            INVALID_RETRY.description = "Invalid call.";
            init = true;
        }
    }

    public interface ThrowHandler {
        void onThrow(Retry retry, Throwable t);
    }

    private Retry() {
    }

    public static <T> T run(Supplier<T> run) {
        if (!init) {
            init = STATIC_FINAL_HOLDER.init;
        }
        return run(run, defaultRunTotalLimit, null);
    }

    public static <T> T run(Supplier<T> run, ThrowHandler throwHandler) {
        if (!init) {
            init = STATIC_FINAL_HOLDER.init;
        }
        return run(run, defaultRunTotalLimit, throwHandler);
    }

    public static <T> T run(Supplier<T> run, int rerunTotalLimit) {
        if (!init) {
            init = STATIC_FINAL_HOLDER.init;
        }
        return run(run, rerunTotalLimit, null);
    }

    public static <T> T run(Supplier<T> run, int rerunTotalLimit, ThrowHandler throwHandler) {
        Retry retry = createRetry(Thread.currentThread().getStackTrace(), 3, null, null);
        retry.running = true;
        retry.setRerunCountLimit(rerunTotalLimit);
        retry.setThrowHandler(throwHandler);
        int rerunTotal = 0;
        do {
            try {
                return run.get();
            } catch (Throwable t) {
                retry.handlerThrow(t, rerunTotal);
            }
        } while (rerunTotal++ < retry.rerunTotalLimit);
        return null;
    }

    public static Retry simpleRunCurrentMethod(Object target, Object... arguments) {
        Retry retry = getInstance(target, arguments);
        retry.runCurrentMethod();
        return retry;
    }

    public static Retry getInstance(Object target, Object... arguments) {
        if (rerunMap == null) {
            synchronized (Retry.class) {
                if (rerunMap == null) {
                    rerunMap = ThreadLocal.withInitial(HashMap::new);
                }
            }
        }
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        int traceIndex = getRerunMethodTraceIndex(stackTraceElements);
        String key = getRetryKey(stackTraceElements[traceIndex]);
        Retry retry = rerunMap.get().get(key);
        if (retry == null) {
            retry = createRetry(stackTraceElements, traceIndex, target, arguments);
            if (retry == null) {
                logError("Failed to create rerun retry.");
                return STATIC_FINAL_HOLDER.INVALID_RETRY;
            }
            retry.key = key;
            rerunMap.get().put(key, retry);
        }

        return retry;
    }

    public static void logError(String format, Object... arguments) {
        if (errorLog) {
            STATIC_FINAL_HOLDER.logger.error(format, arguments);
        }
    }

    public boolean runCurrentMethod() {
        if (!runnable) {
            logError("cannot run: {}", description);
            return false;
        }
        if (running) {
            return false;
        }
        running = true;
        int rerunTotal = 0;
        do {
            try {
                result = method.invoke(target, arguments);
                success = true;
                break;
            } catch (InvocationTargetException e) {
                handlerThrow(e.getTargetException(), rerunTotal);
            } catch (IllegalAccessException e) {
                logError(e.getMessage());
            }
        } while (rerunTotal++ <= rerunTotalLimit);
        clean();
        return success;
    }

    private static Retry createRetry(StackTraceElement[] stackTraceElements, int traceIndex, Object target, Object... arguments) {
        StackTraceElement currentStackTraceElement = stackTraceElements[traceIndex];
        String curClassName = currentStackTraceElement.getClassName();
        String curMethodName = currentStackTraceElement.getMethodName();

        Method method = getCurrentMethod(curClassName, curMethodName, arguments);
        if (method == null) {
            return null;
        }
        try {
            method.setAccessible(true);
        } catch (Throwable t) {
            logError(t.getMessage());
            return null;
        }

        Retry retry = new Retry();
        retry.method = method;
        retry.target = target;
        retry.arguments = arguments;
        retry.rerunTotalLimit = defaultRunTotalLimit;
        retry.runnable = true;
        retry.className = curClassName;
        StringBuilder descBuilder = new StringBuilder()
                .append(curClassName).append(".").append(curMethodName).append(" is called");
        if (traceIndex + 1 < stackTraceElements.length) {
            retry.calledLine = stackTraceElements[traceIndex + 1].getLineNumber();
            for (int i = traceIndex; i < stackTraceElements.length; i++) {
                StackTraceElement element = stackTraceElements[i];
                String className = element.getClassName();
                if (i + 1 < stackTraceElements.length && !isSkipPackage(stackTraceElements[i + 1].getClassName())
                        && className.equals(curClassName) && element.getMethodName().equals(curMethodName)) {
                    descBuilder.append(" on ").append(stackTraceElements[i + 1]);
                    break;
                }
            }
        } else {
            descBuilder.append(" of ").append(currentStackTraceElement.getFileName());
        }
        retry.description = descBuilder.toString();
        return retry;
    }

    private static boolean isSkipPackage(String className) {
        return className.startsWith(STATIC_FINAL_HOLDER.CURRENT_CLASS_NAME)
                || className.startsWith(STATIC_FINAL_HOLDER.PACKAGE_JAVA_LANG)
                || className.startsWith(STATIC_FINAL_HOLDER.PACKAGE_SUN_REFLECT);
    }

    private static String getRetryKey(StackTraceElement stackTraceElement) {
        return new StringBuilder().append(stackTraceElement.getClassName()).append("#")
                .append(stackTraceElement.getMethodName()).toString();
    }

    private static int getRerunMethodTraceIndex(StackTraceElement[] stackTraceElements) {
        int traceIndex = 0;
        for (; traceIndex < stackTraceElements.length; traceIndex++) {
            StackTraceElement element = stackTraceElements[traceIndex];
            if (element.getClassName().endsWith(STATIC_FINAL_HOLDER.CURRENT_CLASS_NAME)
                    && element.getMethodName().equals(STATIC_FINAL_HOLDER.METHOD_NAME_KEY1)) {
                if (++traceIndex < stackTraceElements.length && stackTraceElements[traceIndex].getMethodName().equals(STATIC_FINAL_HOLDER.METHOD_NAME_KEY2)) {
                    ++traceIndex;
                }
                break;
            }
        }
        return traceIndex;
    }

    private static Method getCurrentMethod(String className, String methodName, Object... arguments) {
        Method currentMethod = null;
        try {
            Class<?> clazz = Class.forName(className);
            Method[] methods = clazz.getDeclaredMethods();
            int expectParameterCount = 0;
            if (arguments != null) {
                expectParameterCount = arguments.length;
            }
            Class<?>[] expectParameterTypes = new Class[expectParameterCount];
            for (int i = 0; i < expectParameterCount; i++) {
                if (arguments[i] != null) {
                    expectParameterTypes[i] = arguments[i].getClass();
                }
            }
            int maxMatchCount = 0;
            for (Method method : methods) {
                if (!method.getName().equals(methodName)
                        || expectParameterCount != method.getParameterCount()) {
                    continue;
                }
                if (expectParameterCount == 0) {
                    currentMethod = method;
                    break;
                }

                Class<?>[] parameterTypes = method.getParameterTypes();
                int matchCount = matchParameterTypesCount(expectParameterTypes, parameterTypes, true);
                if (matchCount == expectParameterCount) {
                    currentMethod = method;
                    break;
                }
                if (matchParameterTypesCount(expectParameterTypes, parameterTypes, false) == expectParameterCount) {
                    if (matchCount > maxMatchCount) {
                        maxMatchCount = matchCount;
                        currentMethod = method;
                    }
                }
            }
        } catch (ClassNotFoundException ignored) {
        }
        return currentMethod;
    }

    private static int matchParameterTypesCount(Class<?>[] expect, Class<?>[] actual, boolean accurate) {
        if (expect.length != actual.length) {
            return 0;
        }
        int index = 0;
        for (; index < expect.length; index++) {
            Class<?> expectType = expect[index];
            if (expectType == null) {
                continue;
            }
            Class<?> actualType = actual[index];
            if (isPrimitive(expectType)) {
                expectType = getBaseType(expectType);
            }
            if (isPrimitive(actualType)) {
                actualType = getBaseType(actualType);
            }
            if (accurate && expectType != actualType) {
                break;
            }
            if (!accurate && !expectType.isAssignableFrom(actualType)) {
                break;
            }
        }
        if (index == expect.length) {
            return index;
        }
        return 0;
    }

    private static Class<?>[] baseAndWrapperTypes = new Class[]{
            Boolean.class, boolean.class,
            Character.class, char.class,
            Byte.class, byte.class,
            Short.class, short.class,
            Integer.class, int.class,
            Long.class, long.class,
            Float.class, float.class,
            Double.class, double.class
    };

    private static Class<?> getBaseType(Class<?> type) {
        int end = baseAndWrapperTypes.length - 1;
        for (int i = 0; i < end; i += 2) {
            if (type == baseAndWrapperTypes[i]) {
                return baseAndWrapperTypes[i + 1];
            }
        }
        return null;
    }

    private static boolean isPrimitive(Class<?> clazz) {
        try {
            return ((Class<?>) clazz.getField("TYPE").get(null)).isPrimitive();
        } catch (Exception e) {
            return false;
        }
    }

    private void handlerThrow(Throwable t, int rerunTotal) {
        hasNext = rerunTotal < rerunTotalLimit;
        for (StackTraceElement element : t.getStackTrace()) {
            if (element.getClassName().equals(className)
                    && element.getMethodName().equals(method.getName())) {
                logError("{}\nFailed at {}.{}({}:{}), {}th run, rerun total limit is {}\n{}\n{}",
                        description,
                        element.getClassName(), element.getMethodName(), element.getFileName(), element.getLineNumber(),
                        rerunTotal + 1, rerunTotalLimit,
                        t.getClass().getName(), t.getMessage());
                break;
            }
        }
        if (throwHandler != null) {
            try {
                throwHandler.onThrow(this, t);
            } catch (Throwable throwable) {
                logError("Failed to execute throw handler of {}", description);
            }
        }

        if (!hasNext) {
            clean();
            throw new RuntimeException(t);
        }
    }

    private void clean() {
        Map map = rerunMap.get();
        if (map != null) {
            map.remove(key);
        }
    }

    private Retry setRunnable(boolean runnable) {
        this.runnable = runnable;
        return this;
    }

    public Retry setThrowHandler(ThrowHandler throwHandler) {
        this.throwHandler = throwHandler;
        return this;
    }

    public Retry setRerunCountLimit(int rerunTotalLimit) {
        if (!running) {
            this.rerunTotalLimit = rerunTotalLimit;
        }
        return this;
    }

    public Retry updateArguments(Object... arguments) {
        if (arguments != null) {
            if (this.arguments == null) {
                this.arguments = arguments;
            } else {
                for (int i = 0; i < this.arguments.length && i < arguments.length; i++) {
                    this.arguments[i] = arguments[i];
                }
            }
        }
        return this;
    }

    public Object getResult() {
        runCurrentMethod();
        return result;
    }

    public boolean isRunnable() {
        return runnable;
    }

    public boolean hasNext() {
        return hasNext;
    }

    public boolean isSuccess() {
        return success;
    }

    public ThrowHandler getThrowHandler() {
        return throwHandler;
    }

    public int getRerunTotalLimit() {
        return rerunTotalLimit;
    }

    public Method getMethod() {
        return method;
    }

    public Object getTarget() {
        return target;
    }

    public Object[] getArguments() {
        return arguments;
    }

    public Integer getCalledLine() {
        return calledLine;
    }

    public String getDescription() {
        return description;
    }
}