package io.quarkiverse.messaging.jms.deployment;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;

import io.quarkiverse.messaging.jms.runtime.JmsXaTransactionInvoker;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.GeneratedClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.quarkus.runtime.util.HashUtil;
import io.quarkus.smallrye.reactivemessaging.deployment.items.ConnectorManagedChannelBuildItem;
import io.quarkus.smallrye.reactivemessaging.deployment.items.CustomInvokerBuildItem;
import io.smallrye.reactive.messaging.jms.JmsXaTransactionMetadata;

class SmallRyeReactiveMessagingJmsProcessor {

    private static final DotName INCOMING = DotName.createSimple("org.eclipse.microprofile.reactive.messaging.Incoming");
    private static final String JMS_CONNECTOR = "smallrye-jms";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("messaging-jms");
    }

    @BuildStep
    void generateXaTransactionInvokers(
            CombinedIndexBuildItem combinedIndex,
            List<ConnectorManagedChannelBuildItem> managedChannels,
            BuildProducer<GeneratedClassBuildItem> generatedClass,
            BuildProducer<CustomInvokerBuildItem> customInvokers,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClass) {

        Set<String> xaChannels = findXaChannels(managedChannels);
        if (xaChannels.isEmpty()) {
            return;
        }

        ClassOutput classOutput = (name, data) -> generatedClass.produce(new GeneratedClassBuildItem(true, name, data));

        for (AnnotationInstance incoming : combinedIndex.getIndex().getAnnotations(INCOMING)) {
            if (incoming.target().kind() != org.jboss.jandex.AnnotationTarget.Kind.METHOD) {
                continue;
            }
            String channelName = incoming.value().asString();
            if (!xaChannels.contains(channelName)) {
                continue;
            }

            MethodInfo method = incoming.target().asMethod();
            String generatedName = generateXaInvoker(method, classOutput);
            String invokerClassName = generatedName.replace('/', '.');

            customInvokers.produce(CustomInvokerBuildItem
                    .builder(CustomInvokerBuildItem.mediatorMethodId(method), invokerClassName)
                    .acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
                    .syntheticParameterTypes(List.of(JmsXaTransactionMetadata.class.getName()))
                    .build());
            reflectiveClass.produce(ReflectiveClassBuildItem.builder(invokerClassName).build());
        }
    }

    private Set<String> findXaChannels(List<ConnectorManagedChannelBuildItem> managedChannels) {
        Config config = ConfigProvider.getConfig();
        Set<String> xaChannels = new HashSet<>();
        for (ConnectorManagedChannelBuildItem channel : managedChannels) {
            if (channel.isIncoming() && JMS_CONNECTOR.equals(channel.getConnector())) {
                String txMode = config
                        .getOptionalValue("mp.messaging.incoming." + channel.getName() + ".transaction-mode", String.class)
                        .orElse("none");
                if ("xa".equalsIgnoreCase(txMode)) {
                    xaChannels.add(channel.getName());
                }
            }
        }
        return xaChannels;
    }

    private String generateXaInvoker(MethodInfo method, ClassOutput classOutput) {
        String baseName = method.declaringClass().name().withoutPackagePrefix();
        StringBuilder sigBuilder = new StringBuilder();
        sigBuilder.append(method.name()).append("_").append(method.returnType().name().toString());
        for (var paramType : method.parameterTypes()) {
            sigBuilder.append(paramType.name().toString());
        }
        String targetPackage = method.declaringClass().name().packagePrefix().replace('.', '/') + "/";
        String generatedName = targetPackage + baseName
                + "_JmsXaInvoker_" + method.name() + "_"
                + HashUtil.sha1(sigBuilder.toString());

        String superClass = JmsXaTransactionInvoker.class.getName();
        String beanInstanceType = method.declaringClass().name().toString();

        try (ClassCreator invoker = ClassCreator.builder().classOutput(classOutput).className(generatedName)
                .superClass(superClass)
                .build()) {

            FieldDescriptor beanInstanceField = invoker.getFieldCreator("beanInstance", beanInstanceType)
                    .getFieldDescriptor();

            try (MethodCreator ctor = invoker.getMethodCreator("<init>", void.class, Object.class)) {
                ctor.setModifiers(Modifier.PUBLIC);
                ctor.invokeSpecialMethod(
                        MethodDescriptor.ofConstructor(superClass),
                        ctor.getThis());
                ctor.writeInstanceField(beanInstanceField, ctor.getThis(), ctor.getMethodParam(0));
                ctor.returnValue(null);
            }

            try (MethodCreator invokeBean = invoker.getMethodCreator(
                    MethodDescriptor.ofMethod(generatedName, "invokeBean", Object.class, Object[].class))) {
                invokeBean.setModifiers(Modifier.PROTECTED);

                int parametersCount = method.parametersCount();
                String[] argTypes = new String[parametersCount];
                ResultHandle[] args = new ResultHandle[parametersCount];
                for (int i = 0; i < parametersCount; i++) {
                    args[i] = invokeBean.readArrayValue(invokeBean.getMethodParam(0), i);
                    argTypes[i] = method.parameterType(i).name().toString();
                }
                ResultHandle result = invokeBean.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(beanInstanceType, method.name(),
                                method.returnType().name().toString(), argTypes),
                        invokeBean.readInstanceField(beanInstanceField, invokeBean.getThis()), args);
                if (DotName.createSimple("void").equals(method.returnType().name())) {
                    invokeBean.returnValue(invokeBean.loadNull());
                } else {
                    invokeBean.returnValue(result);
                }
            }
        }
        return generatedName;
    }
}
