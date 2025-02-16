/**
 * Precompiled [polaris-license-report.gradle.kts][Polaris_license_report_gradle] script plugin.
 *
 * @see Polaris_license_report_gradle
 */
public
class PolarisLicenseReportPlugin : org.gradle.api.Plugin<org.gradle.api.Project> {
    override fun apply(target: org.gradle.api.Project) {
        try {
            Class
                .forName("Polaris_license_report_gradle")
                .getDeclaredConstructor(org.gradle.api.Project::class.java, org.gradle.api.Project::class.java)
                .newInstance(target, target)
        } catch (e: java.lang.reflect.InvocationTargetException) {
            throw e.targetException
        }
    }
}
