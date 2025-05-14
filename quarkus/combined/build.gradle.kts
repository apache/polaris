import org.apache.tools.ant.filters.ReplaceTokens
import publishing.GenerateDigest

plugins {
    base
    id("distribution")
}

description = "Apache Polaris Combined Distribution"

val adminProject = project(":polaris-quarkus-admin")
val serverProject = project(":polaris-quarkus-server")

distributions {
    main {
        distributionBaseName.set("polaris-quarkus-combined")
        contents {
            // Copy admin distribution contents
            into("admin") {
                from(adminProject.layout.buildDirectory.dir("quarkus-app"))
            }

            // Copy server distribution contents
            into("server") {
                from(serverProject.layout.buildDirectory.dir("quarkus-app"))
            }

            from("scripts/run.sh")
            from("distribution/README.md")

            // TODO: combine the LICENSE and NOTICE in a follow-up PR
            from("${adminProject.projectDir}/distribution/NOTICE")
            from("${adminProject.projectDir}/distribution/LICENSE")
        }
    }
}

val distTar = tasks.named<Tar>("distTar") {
    dependsOn(":polaris-quarkus-admin:quarkusBuild", ":polaris-quarkus-server:quarkusBuild")
    inputs.files(adminProject.layout.buildDirectory.dir("quarkus-app"))
    inputs.files(serverProject.layout.buildDirectory.dir("quarkus-app"))
    compression = Compression.GZIP
}

val distZip = tasks.named<Zip>("distZip") {
    dependsOn(":polaris-quarkus-admin:quarkusBuild", ":polaris-quarkus-server:quarkusBuild")
    inputs.files(adminProject.layout.buildDirectory.dir("quarkus-app"))
    inputs.files(serverProject.layout.buildDirectory.dir("quarkus-app"))
}

val digestDistTar =
    tasks.register<GenerateDigest>("digestDistTar") {
        description = "Generate the distribution tar digest"
        mustRunAfter(distTar)
        file.set { distTar.get().archiveFile.get().asFile }
    }

val digestDistZip =
    tasks.register<GenerateDigest>("digestDistZip") {
        description = "Generate the distribution zip digest"
        mustRunAfter(distZip)
        file.set { distZip.get().archiveFile.get().asFile }
    }

distTar.configure { finalizedBy(digestDistTar) }

distZip.configure { finalizedBy(digestDistZip) }

//if (project.hasProperty("release") || project.hasProperty("signArtifacts")) {
//    signing {
//        sign(distTar.get())
//        sign(distZip.get())
//    }
//}