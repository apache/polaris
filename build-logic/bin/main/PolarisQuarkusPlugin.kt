/**
 * Precompiled [polaris-quarkus.gradle.kts][Polaris_quarkus_gradle] script plugin.
 *
 * @see Polaris_quarkus_gradle
 */
public
class PolarisQuarkusPlugin : org.gradle.api.Plugin<org.gradle.api.Project> {
    override fun apply(target: org.gradle.api.Project) {
        try {
            Class
                .forName("Polaris_quarkus_gradle")
                .getDeclaredConstructor(org.gradle.api.Project::class.java, org.gradle.api.Project::class.java)
                .newInstance(target, target)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException
        }
    }
}
