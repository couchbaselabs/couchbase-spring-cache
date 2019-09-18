@Library('jenkins-rhapsody-libraries@master') _ //Importing shared Libraries
import com.rh.rhapsody.*;

rhapsodyUtils.standardPipelineProperties();


Service s = Service.COUCHBASE_SPRING_CACHE;

Java11Pipeline pipeline = new Java11Pipeline(this, s, env);

pipeline.standardTemplate { label ->
  node(label) {
    stage ('Init') {
      pipeline.checkoutCode()
    }
    stage('Build') {
      pipeline.buildMvn();
    } // end stage
    stage('Code Quality') {
      pipeline.codeQuality()
    }//end code quality    
    stage('Publish Maven') {
      pipeline.buildMvn("deploy");
    } // end stage
  } // end Node
} // pipelineTemplate

