pipeline {
    agent any
    stages {
        stage ('Build stage') {
        steps {
            dir('.') {
             git url: 'https://github.com/parbajhos1996/bet-stat.git'
            }
            withMaven() {
		sh 'mvn install'
            }
        }
        }
    }
}
