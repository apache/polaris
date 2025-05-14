import org.apache.tools.ant.filters.ReplaceTokens

plugins {
    id("polaris-distribution")
}

description = "Apache Polaris Combined Distribution"

val adminProject = project(":polaris-quarkus-admin")
val serverProject = project(":polaris-quarkus-server")

// Create a run script that can launch either admin or server
val runScript = tasks.register<Copy>("createRunScript") {
    from("src/main/scripts/run.sh")
    into(layout.buildDirectory.dir("scripts"))
    filter<ReplaceTokens>("tokens" to mapOf("version" to version))
    fileMode = 0b111101101 // 755 in octal
}

distributions {
    main {
        distributionBaseName.set("polaris-quarkus-combined")
        contents {
            // Add run script
            from(runScript)

            // Copy admin distribution contents
            into("admin") {
                from(adminProject.layout.buildDirectory.dir("quarkus-app"))
            }

            // Copy server distribution contents
            into("server") {
                from(serverProject.layout.buildDirectory.dir("quarkus-app"))
            }

            // Add shared files from admin distribution (they're the same in both)
            from("${adminProject.projectDir}/distribution/NOTICE")
            from("${adminProject.projectDir}/distribution/LICENSE")
            from("${adminProject.projectDir}/distribution/README.md")
        }
    }
}

// Make sure the admin and server distributions are built before this one
tasks.named("assembleDist") {
    dependsOn(":polaris-quarkus-admin:build", ":polaris-quarkus-server:build")
} 