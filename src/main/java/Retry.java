import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class Retry {
    private static ThreadLocal<Map<String, Retry>> rerunMap;
    private static boolean errorLog;
    private static int defaultRetryTotal;
    private ThrowHandler throwHandler;
    private int retryTotal;
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
        private static final String METHOD_NAME_KEY3 = "run";
        private static final String PACKAGE_JAVA_LANG = "java.lang";
        private static final String PACKAGE_SUN_REFLECT = "sun.reflect";
        private static final String CURRENT_CLASS_NAME = Retry.class.getName();
        private static final Retry INVALID_RETRY;

        static {
            errorLog = Boolean.parseBoolean(System.getProperty("Retry.log", "true"));
            defaultRetryTotal = Integer.getInteger("Retry.defaultRetryTotal", 3);
            INVALID_RETRY = new Retry();
            INVALID_RETRY.description = "Invalid call.";
        }

        static void init() {
        }
    }

    public interface ThrowHandler {
        void onThrow(Retry retry, Throwable t);
    }

    private Retry() {
    }

    public static <T> T run(Supplier<T> run) {
        STATIC_FINAL_HOLDER.init();
        return run(run, defaultRetryTotal, null);
    }

    public static <T> T run(Supplier<T> run, ThrowHandler throwHandler) {
        STATIC_FINAL_HOLDER.init();
        return run(run, defaultRetryTotal, throwHandler);
    }

    public static <T> T run(Supplier<T> run, int retryTotal) {
        return run(run, retryTotal, null);
    }

    public static <T> T run(Supplier<T> run, int retryTotal, ThrowHandler throwHandler) {
        Retry retry = new Retry();
        retry.setRetryTotal(retryTotal);
        retry.setThrowHandler(throwHandler);
        int throwTotal = 0;
        do {
            try {
                return run.get();
            } catch (Throwable t) {
                retry.handlerThrow(t, ++throwTotal);
            }
        } while (throwTotal <= retry.retryTotal);
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
        int throwTotal = 0;
        do {
            try {
                result = method.invoke(target, arguments);
                success = true;
                break;
            } catch (Throwable e) {
                if (e instanceof InvocationTargetException) {
                    e = ((InvocationTargetException) e).getTargetException();
                }
                handlerThrow(e, ++throwTotal);
            }
        } while (throwTotal++ <= retryTotal);
        clean();
        return success;
    }

    private static Retry createRetry(StackTraceElement[] stackTraceElements, int traceIndex, Object target, Object... arguments) {
        StackTraceElement currentStackTraceElement = stackTraceElements[traceIndex];
        String curClassName = currentStackTraceElement.getClassName();
        String curMethodName = currentStackTraceElement.getMethodName();

        Method method = getRetryMethod(curClassName, curMethodName, arguments);
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
        retry.retryTotal = defaultRetryTotal;
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

    private static Method getRetryMethod(String className, String methodName, Object... arguments) {
        Method bestMatch = null;
        try {
            Class<?> clazz = Class.forName(className);
            Method[] methods = clazz.getDeclaredMethods();
            int parameterCount = 0;
            if (arguments != null) {
                parameterCount = arguments.length;
            }
            Class<?>[] parameterTypes = new Class[parameterCount];
            List<Integer> nullArgIndexList = null;
            for (int i = 0; i < parameterCount; i++) {
                if (arguments[i] != null) {
                    parameterTypes[i] = arguments[i].getClass();
                } else {
                    if (nullArgIndexList == null) {
                        nullArgIndexList = new ArrayList<>(parameterCount);
                    }
                    nullArgIndexList.add(i);
                }
            }

            List<Method> candidateMethods = new ArrayList<>();
            for (Method method : methods) {
                if (!method.getName().equals(methodName)
                        || parameterCount != method.getParameterCount()) {
                    continue;
                }
                if (parameterCount == 0) {
                    bestMatch = method;
                    break;
                }
                candidateMethods.add(method);
                bestMatch = method;
            }

            if (candidateMethods.size() > 1) {
                int minDistance = Integer.MAX_VALUE;
                List<Class<?>>[] parameterTypeSortedArr = null;
                if (nullArgIndexList != null) {
                    parameterTypeSortedArr = new List[parameterCount];
                    for (Integer nullIndex : nullArgIndexList) {
                        List<Class<?>> list = new ArrayList<>(methods.length);
                        for (Method m : candidateMethods) {
                            Class<?> c = m.getParameterTypes()[nullIndex];
                            if (!list.contains(c)) {
                                list.add(c);
                            }
                        }
                        list.sort((a, b) -> {
                            if (a == b) {
                                return 0;
                            }
                            if (a.isAssignableFrom(b)) {
                                return 1;
                            }
                            return -1;
                        });
                        if (list.size() > 1 && list.get(list.size() - 1).isAssignableFrom(list.get(0))) {
                            parameterTypeSortedArr[nullIndex] = list;
                        }
                    }
                }

                for (Method method : candidateMethods) {
                    int distance = getParamTypesDistance(method.getParameterTypes(), parameterTypes, parameterTypeSortedArr);
                    if (distance >= 0 && distance < minDistance) {
                        minDistance = distance;
                        bestMatch = method;
                        if (distance == 0) {
                            break;
                        }
                    }
                }
            }
        } catch (ClassNotFoundException ignored) {
        }
        return bestMatch;
    }

    private static int getParamTypesDistance(Class<?>[] parameterTypes, Class<?>[] actualParameterTypes, List<Class<?>>[] parameterTypeSortedArr) {
        int distance = 0;
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> actualParameterType = actualParameterTypes[i];
            if (actualParameterType == null) {
                Class<?> methodParameterType = parameterTypes[i];
                for (Class<?> c : parameterTypeSortedArr[i]) {
                    if (c != methodParameterType) {
                        ++distance;
                    } else {
                        break;
                    }
                }
                continue;
            }

            if (!parameterTypes[i].isAssignableFrom(actualParameterType)) {
                return -1;
            }

            Class<?> clazz = actualParameterType;
            while (clazz != parameterTypes[i]) {
                Class<?> superClass = clazz.getSuperclass();
                if (superClass != null && superClass.isAssignableFrom(actualParameterType)) {
                    clazz = clazz.getSuperclass();
                } else {
                    Class<?>[] classes = clazz.getInterfaces();
                    for (Class<?> c : classes) {
                        if (c.isAssignableFrom(actualParameterType)) {
                            clazz = c;
                            break;
                        }
                    }
                }
                ++distance;
            }
        }
        return distance;
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

    private void handlerThrow(Throwable t, int throwTotal) {
        hasNext = throwTotal <= retryTotal;
        String className;
        String methodName;
        if (method != null) {
            className = this.className;
            methodName = this.method.getName();
        } else {
            className = STATIC_FINAL_HOLDER.CURRENT_CLASS_NAME;
            methodName = STATIC_FINAL_HOLDER.METHOD_NAME_KEY3;
        }
        StackTraceElement[] stackTraceElements = t.getStackTrace();
        for (int i = 0; i < stackTraceElements.length; i++) {
            StackTraceElement element = stackTraceElements[i];
            if (element.getClassName().equals(className)
                    && element.getMethodName().equals(methodName)) {
                if (method == null) {
                    description = stackTraceElements[i - 1].toString();
                    int index = i - 2;
                    element = stackTraceElements[index < 0 ? 0 : index];
                }
                logError("{}\nFailed at {}.{}({}:{}), {}th throw, retry total limit is {}\n{}\n{}",
                        description,
                        element.getClassName(), element.getMethodName(), element.getFileName(), element.getLineNumber(),
                        throwTotal, retryTotal,
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
        if (rerunMap == null) {
            return;
        }
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

    public Retry setRetryTotal(int retryTotal) {
        if (!running) {
            this.retryTotal = retryTotal;
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
        return retryTotal;
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