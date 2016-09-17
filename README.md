# opendata-ckan-pusher
Project to allow automatic periodic pushing of local data to CKAN

In order to build all project artifacts, a jre needs to be placed in the project home as well as extracted commons-daemon 

### How to build

1. mvn clean package
2. go to target/install
3. create a self-extracting archive with all files form the install directory. Set the installation path to "program files\opendata-pusher" 

### How to generate doc file with documentation

    cd docs/bg
    pandoc -s -i instructions.md -f markdown+compact_definition_lists -o Instructions.docx