pipeline {
    agent any
    stages {
        stage ('Compile Stage') {
        steps {
            dir('.') {
             git url: 'https://github.com/parbajhos1996/bet-stat.git'
            }
            withMaven() {
                sh 'mvn clean install'
            }
        }
        }
    }
}
