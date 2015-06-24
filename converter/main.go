package main

import (
    "flag"
    "fmt"
    "os"
    "path/filepath"
    
)

var data_root_path = flag.String("root",".","the root path of csv files")


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
  if len(os.Args) != 3 {
    fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
    flag.PrintDefaults()
    return
  }
  flag.Parse()
  
  fi, err := os.Stat(*data_root_path)
  if err == nil {
     if ! fi.IsDir() {
        fmt.Printf(" %s is not a valid path\n", *data_root_path)
        return
     }
  }else{
     fmt.Printf(" failed to open %s , Error is \"%s\" \n", *data_root_path, err)
     return 
  }
    
  
  e := filepath.Walk(*data_root_path, listFiles)
  if e != nil {
      fmt.Println("failed to parse the root folder")  
  }
}
