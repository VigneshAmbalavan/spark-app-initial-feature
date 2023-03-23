pipeline {
  agent {dockerfile {
  args "-u jenkins"}
  }
  stages {
    stage("") {
      steps {
        script{
        sh "pipenv install --dev"
        }
      }
    }
    stage("test"){
      steps{
        sh "pipenv run python -m unittest tests/test_hf_recipe_preprocess_etl.py"
        sh "pipenv run python -m unittest tests/test_hf_recipe_ingred_report_etl.py"
      }
    }
    stage("prepare artifact"){
      steps{
        sh "make build"
      }
    }
    stage("publish artifact"){
      steps{
        sh "aws s3 cp packages.zip s3://some-s3-path/"
      }
    }
  }
}