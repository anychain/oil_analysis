package main

import (
    "fmt"
    "os"
    "path/filepath"
    
)





func listFiles(path string, f os.FileInfo, err error) error {
    if ( f == nil ) {return err}
    if f.IsDir() {return nil}
    matched, _ := filepath.Match("disposition*.csv", filepath.Base(path))
    if matched {
        e := handleDisposition(path)
        if e != nil{
            fmt.Println("failed to deal with ", path)
        }
    }
    
    matched, _ = filepath.Match("lease*.csv", filepath.Base(path))
    if matched {
        e := handleLease(path) 
        if e != nil {
            fmt.Println("failed to deal with ", path)
        }
    }
    matched, _ = filepath.Match("production*.csv", filepath.Base(path))
    if matched {
        e := handleProduction(path)
        if e != nil{
            fmt.Println("failed to deal with ", path)
        }
    }
    return nil
}

/*Parse the root folder to find out all csv files and 
  convert according to predefined rules
*/
func main(){
  root_folder := "/Users/hill/oil/test"
  e := filepath.Walk(root_folder, listFiles)
  if e != nil {
      fmt.Println("failed to parse the root folder")  
  }
}
