pipeline {
    agent any

    triggers {
        // 每5分钟检查一次 GitHub 是否有新提交，有则自动构建
        pollSCM('H/5 * * * *')
    }

    stages {
        stage('build') {
            steps {
                bat 'mvn --version'
            }
        }
    }
}