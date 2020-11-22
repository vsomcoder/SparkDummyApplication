pipeline {
    agent any
    tools {
        maven 'maven363'
    }
    environment {
        ITVERSITY = credentials('itversity')
    }
    stages {
        stage('Compile') { 
            steps {
                sh 'cd SparkWordCount && mvn clean compile' 
            }
        }
        stage('Unit Test') { 
            steps {
                sh 'cd SparkWordCount && mvn clean test'
            }
        }
        stage('Package') { 
            steps {
                sh 'cd SparkWordCount && mvn clean package'
            }
        }
        stage('Deploy') {
            parallel {
                stage('gw02') {
                    steps {
                        sh 'sshpass -p $ITVERSITY_PSW ssh -o StrictHostKeyChecking=no $ITVERSITY_USR@gw02.itversity.com hostname'
                        sh '''sshpass -p $ITVERSITY_PSW sftp -oBatchMode=no -b - $ITVERSITY_USR@gw02.itversity.com << !
                           put SparkWordCount/target/SparkWordCount-1.0-SNAPSHOT.jar
                           bye
                        !'''
                    }
                }
                stage('gw03') {
                    steps {
                        sh 'sshpass -p $ITVERSITY_PSW ssh -o StrictHostKeyChecking=no $ITVERSITY_USR@gw03.itversity.com hostname'
                        sh '''sshpass -p $ITVERSITY_PSW sftp -oBatchMode=no -b - $ITVERSITY_USR@gw03.itversity.com << !
                           put SparkWordCount/target/SparkWordCount-1.0-SNAPSHOT.jar
                           bye
                        !'''
                    }
                }
            }
        }       
    }
}

