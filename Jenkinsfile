#!groovy

// Copyright 2019 Blockchain Technology Partners
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ------------------------------------------------------------------------------


pipeline {
  agent { node { label 'worker' } }

  options {
    ansiColor('xterm')
    timestamps()
    disableConcurrentBuilds()
    buildDiscarder(logRotator(daysToKeepStr: '31'))
  }

  environment {
    ISOLATION_ID = sh(returnStdout: true, script: 'echo $BUILD_TAG | sha256sum | cut -c1-32').trim()
    PROJECT_ID = sh(returnStdout: true, script: 'echo $BUILD_TAG | sha256sum | cut -c1-32').trim()
    LEDGER_NAME = sh(returnStdout: true, script: 'echo $BUILD_TAG | sha256sum | cut -c1-32').trim()
    AWS_REGION = "us-east-1"
  }

  stages {
    stage('Fetch Tags') {
      steps {
        checkout([$class                           : 'GitSCM', branches: [[name: "${GIT_BRANCH}"]],
                  doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [],
                  userRemoteConfigs                : [[credentialsId: 'github-credentials', noTags: false, url: "${GIT_URL}"]],
                  extensions                       : [
                    [$class : 'CloneOption',
                     shallow: false,
                     noTags : false,
                     timeout: 60]
                  ]])
      }
    }

    stage('Build') {
      steps {
        configFileProvider([configFile(fileId: 'global-maven-settings', variable: 'MAVEN_SETTINGS')]) {
          sh '''
            make clean build
          '''
        }
      }
    }

    stage('Package') {
      steps {
        configFileProvider([configFile(fileId: 'global-maven-settings', variable: 'MAVEN_SETTINGS')]) {
          sh '''
            make package
          '''
        }
      }
    }

    stage("Analyze") {
      steps {
        withCredentials([string(credentialsId: 'fossa.full.token', variable: 'FOSSA_API_KEY')]) {
          withSonarQubeEnv('sonarcloud') {
            configFileProvider([configFile(fileId: 'global-maven-settings', variable: 'MAVEN_SETTINGS')]) {
              sh '''
                make analyze
              '''
            }
          }
        }
        // TODO for now don't abort the pipeline
        waitForQualityGate abortPipeline: false
      }
    }

    stage('Test') {
      steps {
        withCredentials([[$class           : 'AmazonWebServicesCredentialsBinding',
                          accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                          credentialsId    : 'a61234f8-c9f7-49f3-b03c-f31ade1e885a',
                          secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
          configFileProvider([configFile(fileId: 'global-maven-settings', variable: 'MAVEN_SETTINGS')]) {
            sh '''
              make test
            '''
          }
          step([$class: "TapPublisher", testResults: "build/daml-test.results"])
        }
      }
    }

    stage('Create Archives') {
      steps {
        configFileProvider([configFile(fileId: 'global-maven-settings', variable: 'MAVEN_SETTINGS')]) {
          sh '''
            make archive
          '''
        }
        archiveArtifacts 'build/*.tgz, build/*.zip'
      }
    }

    stage("Publish") {
      when {
        expression { env.BRANCH_NAME == "master" }
      }
      steps {
        configFileProvider([configFile(fileId: 'global-maven-settings', variable: 'MAVEN_SETTINGS')]) {
          sh '''
            make publish
          '''
        }
      }
    }
  }

  post {
    always {
      withCredentials([[$class           : 'AmazonWebServicesCredentialsBinding',
                        accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                        credentialsId    : 'a61234f8-c9f7-49f3-b03c-f31ade1e885a',
                        secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
        sh '''
            make clean_aws
          '''
      }
    }
    success {
      echo "Successful build"
    }
    aborted {
      error "Aborted, exiting now"
    }
    failure {
      error "Failed, exiting now"
    }
  }
}
