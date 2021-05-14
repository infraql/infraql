package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/hashicorp/hcl"
)

func main() {

	//Define Commands
	showCommand := flag.NewFlagSet("show", flag.ExitOnError)
	selectCommand := flag.NewFlagSet("select", flag.ExitOnError)

	//Define flags for each command
	showPath := showCommand.String("path", "", `Input the path to a tf file, and names 
	of all resources will be returned`)

	selectPath := selectCommand.String("path", "", `Enter the path to a tf file using -path. 
	Correct syntax is -flag=path, -flag path. If the resource name is entered,
	a flattened json response of all the resource information is returned. \n`)
	selectResourceName := selectCommand.String("resource", "", `Enter the name of the resource, 
	-resource is a required flag`)
	flag.Parse()

	// os.Args = 1 indicates no command was typed
	if len(os.Args) == 1 {
		fmt.Println("usage: readplan <command>  optional [<args>]")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "show":
		showCommand.Parse(os.Args[2:])

		if *showPath == "" {
			fmt.Printf("Please enter a path location by using the -path flag.\n")
			os.Exit(1)
		}

		data, err := readAndUnmarshalFile(*showPath)
		check(err)
		findAllResources(data)
	case "select":
		selectCommand.Parse(os.Args[2:])

		if *selectPath == "" || *selectResourceName == "" {
			fmt.Printf("-path and -resource are both mandatory flags. See -help for more info. \n")
			os.Exit(1)
		}

		data, err := readAndUnmarshalFile(*selectPath)
		check(err)
		getResourceInfo(*selectResourceName, data)
	default:
		fmt.Printf("%q is not valid command.\n", os.Args[1])
		os.Exit(1)
	}
}

func findAllResources(data map[string]interface{}) {
	// store resource names in a slice
	sliceOfResources := []string{}

	resources, resourcesDoExist := data["resources"]
	if !resourcesDoExist {
		check(errors.New("no recources in the tf file"))
	}

	for _, resource := range resources.([]map[string]interface{}) {
		if name, nameExists := resource["name"]; nameExists {
			sliceOfResources = append(sliceOfResources, name.(string))
		} else {
			fmt.Println("Warning: one of the resources does not have a name.")
		}
	}

	json, err := json.MarshalIndent(sliceOfResources, "", "	")
	check(err)
	fmt.Println(string(json))
}

func getResourceInfo(name string, data map[string]interface{}) {

	resourceInfo := []map[string]interface{}{}
	resources, resourcesDoExist := data["resources"]

	if !resourcesDoExist {
		check(errors.New("no recources in the tf file"))
	}

	// Iterate through all the recources and store their name in a slice
	for _, resource := range resources.([]map[string]interface{}) {

		if resource["name"].(string) == name {
			resourceInfo = append(resourceInfo, resource)
		}
	}

	switch len(resourceInfo) {
	case 0:
		check(errors.New("there is no resource with that name"))
	case 1:
		json, err := json.MarshalIndent(resourceInfo, "", " ")
		check(err)
		fmt.Println(string(json))
	default:
		fmt.Printf("There were %d resources found with the name %s \n", len(resourceInfo), name)
		for _, resource := range resourceInfo {
			json, err := json.MarshalIndent(resource, "", " ")
			check(err)
			fmt.Println(string(json) + "\n \n")
		}
	}
}

func readAndUnmarshalFile(path string) (map[string]interface{}, error) {
	// store data in a map
	var data map[string]interface{}

	if !fileExists(path) {
		check(errors.New("the file does not exist or the wrong path was given"))
	}
	fmt.Println(path)
	fmt.Println(filepath.Ext(path))
	if filepath.Ext(path) != ".tf" {
		check(errors.New("only files with .tf extension are accepted"))
	}

	input, err := ioutil.ReadFile(path)
	check(err)

	err = hcl.Unmarshal(input, &data)
	check(err)
	return data, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func check(e error) {
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
}
