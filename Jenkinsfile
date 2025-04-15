pipeline {
    agent {
          label 'agent-image-jdk17'
    }

    parameters {
        string(name: 'BASE_VERSION', defaultValue: '1.2.5-SNAPSHOT', description: 'Base version (e.g., 2.1.0 or 2.1.0-SNAPSHOT)')
        string(name: 'MAVEN_PROFILE', defaultValue: 'spark34', description: 'Maven build profile to use')
    }

    environment {
        NEXUS_CREDENTIALS_ID = 'dnet-nexus-creds'
        AGENT_LABEL = (params.MAVEN_PROFILE == 'spark24' ? 'agent-image-jdk8' : 'agent-image-jdk17')
    }

    stages {
        stage('Checkout') {
            agent AGENT_LABEL
            steps {
                git url: 'https://code-repo.d4science.org/D-Net/dnet-hadoop.git', branch: "${env.BRANCH_NAME}"
            }
        }

        stage('Set Version with Branch and Profile') {
            agent AGENT_LABEL
            when {
                // Only say hello if a "greeting" is requested
                expression { params.MAVEN_PROFILE != 'spark24' }
            }
            steps {
                script {
                    def branch = env.BRANCH_NAME ?: env.GIT_BRANCH ?: 'local'
                    def safeBranch = branch.replaceAll(/[^a-zA-Z0-9\-]/, '-')

                    def isSnapshot = params.BASE_VERSION.endsWith("-SNAPSHOT")
                    def base = params.BASE_VERSION.replace("-SNAPSHOT", "")
                    def version = isSnapshot ?
                        "${base}.${params.MAVEN_PROFILE}-SNAPSHOT" :
                        "${base}.${params.MAVEN_PROFILE}"

                    echo "🔖 Setting full version to: ${version}"

                    sh "java --source 17 dhp-common/src/main/java/ChangeProfile.java --activate ${params.MAVEN_PROFILE} --deactivate spark24,spark34,spark35"
                    sh "mvn versions:set -DnewVersion=${version} -DgenerateBackupPoms=false"
                }
            }
        }

        stage('Build with Maven Profile') {
            agent AGENT_LABEL
            steps {
                withMaven {
                    sh "mvn clean package -P${params.MAVEN_PROFILE} --fail-never"
                }
            }
        }

        stage('Deploy to Nexus') {
            agent AGENT_LABEL
            steps {
                withCredentials([usernamePassword(
                    credentialsId: "${NEXUS_CREDENTIALS_ID}",
                    usernameVariable: 'NEXUS_USERNAME',
                    passwordVariable: 'NEXUS_PASSWORD')]) {

                    sh "mvn deploy -s m2.settings.xml -P${params.MAVEN_PROFILE}  -DskipTests"
                }
            }
        }
    }

    post {
        success {
            echo '✅ Build and deployment succeeded!'
        }
        failure {
            echo '❌ Build or deployment failed.'
        }
    }
}
