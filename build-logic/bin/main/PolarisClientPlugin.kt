/**
 * Precompiled [polaris-client.gradle.kts][Polaris_client_gradle] script plugin.
 *
 * @see Polaris_client_gradle
 */
public
class PolarisClientPlugin : org.gradle.api.Plugin<org.gradle.api.Project> {
    override fun apply(target: org.gradle.api.Project) {
        try {
            Class
                .forName("Polaris_client_gradle")
                .getDeclaredConstructor(org.gradle.api.Project::class.java, org.gradle.api.Project::class.java)
                .newInstance(target, target)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException
        }
    }
}
