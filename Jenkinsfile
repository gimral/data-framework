properties([buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '', daysToKeepStr: '5', numToKeepStr: '5'))])
def allow_insecure = '-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true'

node('maven') {
    stage('Git Clone') {
        dir('leap-data-framework') {
            checkout scm
        }
    }

    stage('Build') {
        dir('leap-data-framework') {
            sh """mvn clean install -Dmaven.test.skip=true ${allow_insecure}"""
        }
    }

    stage('Test') {
       dir('leap-data-framework') {
            sh """mvn test ${allow_insecure}"""
        }
    }

    stage('Nexus') {
       dir('leap-data-framework') {
            //def files = findFiles(glob: 'target/*pom.xml') 
            def parentPom = readMavenPom file: "pom.xml"
            def parentVersion = parentPom.version
            def parentGroupId = parentPom.groupId
            def files = findFiles(glob: '**/pom.xml') 
            files.each{
                def pom = readMavenPom file: it.path
                def directory = it.path.replace("/pom.xml","")
                def mavenAssetList = []
                def version = pom.version
                if(pom.version == null){
                    version = parentVersion
                }
                if(pom.packaging == "pom")
                {
                    nexusPublisher nexusInstanceId: 'Nexus', nexusRepositoryId: 'maven-releases', packages: [[$class: 'MavenPackage', 
                    mavenAssetList: [
                        [
                            classifier: '', 
                            extension: 'pom', 
                            filePath: it.path
                        ]
                    ], 
                    mavenCoordinate: [
                        artifactId: pom.artifactId, 
                        groupId: parentGroupId, 
                        packaging: pom.packaging, 
                        version: version
                    ]]]
                }
                else{
                    def jarPath = """${directory}/target/${pom.artifactId}.${version}.jar"""
                    nexusPublisher nexusInstanceId: 'Nexus', nexusRepositoryId: 'maven-releases', packages: [[$class: 'MavenPackage', 
                    mavenAssetList: [
                        [
                            classifier: '', 
                            extension: 'pom', 
                            filePath: it.path
                        ],
                        [
                            classifier: '', 
                            extension: 'jar', 
                            filePath: it.path
                        ]
                    ], 
                    mavenCoordinate: [
                        artifactId: pom.artifactId, 
                        groupId: parentGroupId, 
                        packaging: pom.packaging, 
                        version: version
                    ]]]
                }
            }
            
        }
    }
    

    // def list = []

    // def dir = new File("target")
    // dir.eachFileRecurse (FileType.FILES) { file ->
    //   if file.
    //   list << file
    // }

    // stage('Push Nexus') {
    //     dir('leap-data-framework') {
    //         sh """mvn deploy -Dmaven.install.skip=true -Dmaven.test.skip=true ${allow_insecure}"""
    //     }
    // }
}