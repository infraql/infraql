# Readplan 
Readplan is a command line application in go that has two commands : show and select. 

The Show command takes in a mandatory path argument, and returns the names of all the resources in the tf file. 

The Select command takes in two mandatory arguments, the path to a tf file and a resource name, and returns all the information about that resource. 

Example command line input would be 

readplan show -path C:/projects/terraformfile.tf

or using the current directory readplan show -path terraformStateFile.tf

readplan select -path "pathlocation" -resource nameOfResource


