import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OnThrowRerunService {
    private static ThreadLocal<Map<String, OnThrowRerunService>> rerunMap;
    private static boolean errorLog;
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
        private static final Logger logger = LoggerFactory.getLogger(OnThrowRerunService.class);
        private static final String METHOD_NAME_KEY1 = "getInstance";
        private static final String METHOD_NAME_KEY2 = "simpleRunCurrentMethod";
        private static final String PACKAGE_JAVA_LANG = "java.lang";
        private static final String PACKAGE_SUN_REFLECT = "sun.reflect";
        private static final String SERVICE_CLASS_NAME = OnThrowRerunService.class.getName();
        private static final OnThrowRerunService INVALID_SERVICE;

        static {
            errorLog = Boolean.parseBoolean(System.getProperty("OnThrowRerunService.log", "true"));
            defaultRunTotalLimit = Integer.getInteger("OnThrowRerunService.defaultRerunTotalLimit", 3);
            INVALID_SERVICE = new OnThrowRerunService();
            INVALID_SERVICE.description = "Invalid call.";
        }
    }

    public interface ThrowHandler {
        void onThrow(OnThrowRerunService service, Throwable t);
    }

    private OnThrowRerunService() {
    }

    public static OnThrowRerunService simpleRunCurrentMethod(Object target, Object... arguments) {
        OnThrowRerunService service = getInstance(target, arguments);
        service.runCurrentMethod();
        return service;
    }

    public static OnThrowRerunService getInstance(Object target, Object... arguments) {
        if (rerunMap == null) {
            synchronized (OnThrowRerunService.class) {
                if(rerunMap == null) {
                    rerunMap = ThreadLocal.withInitial(HashMap::new);
                }
            }
        }
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        int traceIndex = getRerunMethodTraceIndex(stackTraceElements);
        String key = getServiceKey(stackTraceElements, traceIndex);
        OnThrowRerunService service = rerunMap.get().get(key);
        if (service == null) {
            service = createService(stackTraceElements, traceIndex, target, arguments);
            if (service == null) {
                logError("Failed to create rerun service.");
                return STATIC_FINAL_HOLDER.INVALID_SERVICE;
            }
            service.key = key;
            rerunMap.get().put(key, service);
        }

        return service;
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

    private static OnThrowRerunService createService(StackTraceElement[] stackTraceElements, int traceIndex, Object target, Object... arguments) {
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


        OnThrowRerunService service = new OnThrowRerunService();
        service.method = method;
        service.target = target;
        service.arguments = arguments;
        service.rerunTotalLimit = defaultRunTotalLimit;
        service.runnable = true;
        service.className = curClassName;
        StringBuilder descBuilder = new StringBuilder()
                .append(curClassName).append(".").append(curMethodName).append(" is called");
        if (traceIndex + 1 < stackTraceElements.length) {
            service.calledLine = stackTraceElements[traceIndex + 1].getLineNumber();
            for (int i = traceIndex; i < stackTraceElements.length; i++) {
                StackTraceElement element = stackTraceElements[i];
                String className = element.getClassName();
                if (i + 1 < stackTraceElements.length && !isSkipPackage(stackTraceElements[i+1].getClassName())
                        && className.equals(curClassName) && element.getMethodName().equals(curMethodName)) {
                    descBuilder.append(" on ").append(stackTraceElements[i + 1]);
                    break;
                }
            }
        } else {
            descBuilder.append(" of ").append(currentStackTraceElement.getFileName());
        }
        service.description = descBuilder.toString();
        return service;
    }

    private static boolean isSkipPackage(String className) {
        return className.startsWith(STATIC_FINAL_HOLDER.SERVICE_CLASS_NAME)
                || className.startsWith(STATIC_FINAL_HOLDER.PACKAGE_JAVA_LANG)
                || className.startsWith(STATIC_FINAL_HOLDER.PACKAGE_SUN_REFLECT);
    }

    private static String getServiceKey(StackTraceElement[] stackTraceElements, int traceIndex) {
        String className = stackTraceElements[traceIndex].getClassName();
        String methodName = stackTraceElements[traceIndex].getMethodName();
        StringBuilder keyBuilder = new StringBuilder().append(className).append("#").append(methodName);
        if (traceIndex == stackTraceElements.length - 1) {
            return keyBuilder.toString();
        }
        int end = 0;
        for (int i = stackTraceElements.length - 1; i > traceIndex; i--) {
            StackTraceElement element = stackTraceElements[i];
            if (element.getClassName().endsWith(className) && element.getMethodName().endsWith(methodName)) {
                end = i;
                break;
            }
        }
        for (int i = traceIndex + 1; i < end; i++) {
            StackTraceElement element = stackTraceElements[i];
            if (isSkipPackage(element.getClassName())) {
                continue;
            }
            keyBuilder.append(":").append(element.getLineNumber());
        }
        return keyBuilder.toString();
    }

    private static int getRerunMethodTraceIndex(StackTraceElement[] stackTraceElements) {
        int traceIndex = 0;
        for (; traceIndex < stackTraceElements.length; traceIndex++) {
            StackTraceElement element = stackTraceElements[traceIndex];
            if (element.getClassName().endsWith(STATIC_FINAL_HOLDER.SERVICE_CLASS_NAME)
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
            List<Type> currentMethodParameterTypes = null;
            int expectParameterCount = 0;
            if (arguments != null && arguments.length > 0) {
                currentMethodParameterTypes = Stream.of(arguments)
                        .map(item -> {
                            if (item != null) {
                                return item.getClass();
                            } else {
                                return null;
                            }
                        }).collect(Collectors.toList());
            }
            if (currentMethodParameterTypes != null) {
                expectParameterCount = currentMethodParameterTypes.size();
            }

            for (Method method : methods) {
                if (!method.getName().equals(methodName)
                        || expectParameterCount != method.getParameterCount()) {
                    continue;
                }
                if (expectParameterCount == 0) {
                    currentMethod = method;
                    break;
                }
                List<Type> parameterTypes = Stream.of(method.getGenericParameterTypes()).collect(Collectors.toList());
                int index = 0;
                for (; index < expectParameterCount && index < parameterTypes.size(); index++) {
                    Type type = currentMethodParameterTypes.get(index);
                    if (type != null && type != parameterTypes.get(index)) {
                        break;
                    }
                }
                if (index == expectParameterCount) {
                    currentMethod = method;
                    break;
                }
            }
        } catch (ClassNotFoundException ignored) {
        }
        return currentMethod;
    }

    private void handlerThrow(Throwable t, int rerunTotal) {
        hasNext = rerunTotal < rerunTotalLimit;
        logError("<");
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
        logError(">");
    }

    private void clean() {
        Map map = rerunMap.get();
        if (map != null) {
            map.remove(key);
        }
    }

    private OnThrowRerunService setRunnable(boolean runnable) {
        this.runnable = runnable;
        return this;
    }

    public OnThrowRerunService setThrowHandler(ThrowHandler throwHandler) {
        this.throwHandler = throwHandler;
        return this;
    }

    public OnThrowRerunService setRerunCountLimit(int rerunTotalLimit) {
        if (!running) {
            this.rerunTotalLimit = rerunTotalLimit;
        }
        return this;
    }

    public OnThrowRerunService updateArguments(Object... arguments) {
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

    public <R> R getResult(Class<R> rClass) {
        runCurrentMethod();
        if (result != null && rClass != null && rClass.isAssignableFrom(result.getClass())) {
            return rClass.cast(result);
        }
        return null;
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
