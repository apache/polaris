/**
 * Precompiled [polaris-shadow-jar.gradle.kts][Polaris_shadow_jar_gradle] script plugin.
 *
 * @see Polaris_shadow_jar_gradle
 */
public
class PolarisShadowJarPlugin : org.gradle.api.Plugin<org.gradle.api.Project> {
    override fun apply(target: org.gradle.api.Project) {
        try {
            Class
                .forName("Polaris_shadow_jar_gradle")
                .getDeclaredConstructor(org.gradle.api.Project::class.java, org.gradle.api.Project::class.java)
                .newInstance(target, target)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException
        }
    }
}
