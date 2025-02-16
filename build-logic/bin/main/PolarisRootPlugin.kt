/**
 * Precompiled [polaris-root.gradle.kts][Polaris_root_gradle] script plugin.
 *
 * @see Polaris_root_gradle
 */
public
class PolarisRootPlugin : org.gradle.api.Plugin<org.gradle.api.Project> {
    override fun apply(target: org.gradle.api.Project) {
        try {
            Class
                .forName("Polaris_root_gradle")
                .getDeclaredConstructor(org.gradle.api.Project::class.java, org.gradle.api.Project::class.java)
                .newInstance(target, target)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException
        }
    }
}
