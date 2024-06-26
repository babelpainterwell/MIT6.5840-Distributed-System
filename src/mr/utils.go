package mr

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

// ==================================================================================
// This section of code is adapted from Github by GFX9.
// Original source: [https://github.com/GFX9/MIT6.5840/blob/lab1/src/mr/utils.go]
// ==================================================================================

// DelFileByMapId deletes files generated by the map task with the given map task ID.
// In case that some data have been written but the task is not completed, this function will delete the files.
func DelFileByMapId(targetNumber int, path string) error {
	// Compile a regular expression pattern to match files with the specified map task ID
	pattern := fmt.Sprintf(`^mr-out-%d-\d+$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	// Read the directory specified by the path
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// Iterate through each file in the directory
	for _, file := range files {
		// Skip directories
		if file.IsDir() {
			continue
		}
		// Get the file name
		fileName := file.Name()
		// Check if the file name matches the regex pattern
		if regex.MatchString(fileName) {
			// Construct the full file path
			filePath := filepath.Join(path, file.Name())
			// Delete the file
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// DelFileByReduceId deletes files generated by the reduce task with the given reduce task ID.
func DelFileByReduceId(targetNumber int, path string) error {
	// Compile a regular expression pattern to match files with the specified reduce task ID
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	// Read the directory specified by the path
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// Iterate through each file in the directory
	for _, file := range files {
		// Skip directories
		if file.IsDir() {
			continue
		}
		// Get the file name
		fileName := file.Name()
		// Check if the file name matches the regex pattern
		if regex.MatchString(fileName) {
			// Construct the full file path
			filePath := filepath.Join(path, file.Name())
			// Delete the file
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ReadSpecificFile reads files generated by the reduce task with the given reduce task ID and returns a list of open file pointers.
func ReadSpecificFile(targetNumber int, path string) (fileList []*os.File, err error) {
	// Compile a regular expression pattern to match files with the specified reduce task ID
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// Read the directory specified by the path
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// Iterate through each file in the directory
	for _, fileEntry := range files {
		// Skip directories
		if fileEntry.IsDir() {
			continue
		}
		// Get the file name
		fileName := fileEntry.Name()
		// Check if the file name matches the regex pattern
		if regex.MatchString(fileName) {
			// Construct the full file path
			filePath := filepath.Join(path, fileEntry.Name())
			// Open the file
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("cannot open %v", filePath)
				// Close all previously opened files in case of an error
				for _, oFile := range fileList {
					oFile.Close()
				}
				return nil, err
			}
			// Add the opened file to the file list
			fileList = append(fileList, file)
		}
	}
	return fileList, nil
}