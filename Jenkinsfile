pipeline {
    agent any
    stages {
        stage('build') {
            steps {
                bat 'mvn --version'   // Windows 上用 bat 而不是 sh
            }
        }
    }
}