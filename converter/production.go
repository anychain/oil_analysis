package main

import (
    "fmt"
    "os"
    "io"
    "bufio"
    "strings"
    "encoding/csv"
    "encoding/json"
    "strconv"
    "io/ioutil"
    
)


/*
CSV reader will ignore the header
*/

type ProductionDetail struct{
    Date           string
    LeaseName      string
    LeaseNo        string
    District       string
    WellNo         string
    Oil            int
    Cashinghead    int
    GWGas          int
    Condensate     int
}
type Production struct{
    InitView             string
    WellType             string
    District             string
    DateRange            string
    ProductionDetail     []ProductionDetail
}


func handleProduction(production_file string) error{

    fmt.Printf("Begin to deal with production file %s \n", production_file)
    file, err := os.Open(production_file)
    if err != nil {
      fmt.Printf("failed to open file %s", production_file)
      return nil
    }
    defer file.Close()
    header_reader := bufio.NewReader(file)
    var production = new(Production) 
    for {
       line, err := header_reader.ReadString('\n')
       
       if err == io.EOF{
           fmt.Println("get the end of file ")
           break
       }else if err == nil {
	       if strings.Index(line, "Initial View:") == 1{
	           production.InitView = line[14:strings.LastIndex(line,"\"")]
	       }else if strings.Index(line, "Well Type:") == 1{
	           production.WellType = strings.TrimSpace(line[11:strings.LastIndex(line,"\"")])
	       }else if strings.Index(line, "District:") == 1 {
	           production.District = line[10:strings.LastIndex(line,"\"")]
	       }else if strings.Index(line, "Date Range:") == 1 {
	           production.DateRange = line[13:strings.LastIndex(line,"\"")]
	           //go back to the head
	           file.Seek(0,0)
	           break
	       } 
       }
    }
    
    
    production.ProductionDetail = make([]ProductionDetail,0)
    
    reader := csv.NewReader(file)
    for{
    
      record, err := reader.Read()
      if err == io.EOF {
          fmt.Println("Read end of the file", production_file)
          break
      }else if err != nil {
          //skip the string with empty first element
          if len(strings.Trim(record[0]," ")) == 0{
              fmt.Println("skip the first empty field")
          }else if record[0] == "Lease Name" {
          }else if record[0] == "Total" {
            //skip the total line
          }else{
             tmp := new(ProductionDetail)
             tmp.Date = strings.Split(production.DateRange,"-")[0]
             tmp.LeaseName         = record[0]
             tmp.LeaseNo           = record[1]
             tmp.District          = record[2]
             tmp.WellNo            = record[3]
             tmp.Oil, _            = strconv.Atoi(strings.Replace(record[4],",","",-1))
             tmp.Cashinghead, _    = strconv.Atoi(strings.Replace(record[5],",","",-1))
             tmp.GWGas, _          = strconv.Atoi(strings.Replace(record[6],",","",-1))
             tmp.Condensate, _     = strconv.Atoi(strings.Replace(record[7],",","",-1))
             production.ProductionDetail = append(production.ProductionDetail, *tmp)
          }
      }
    }
    
    b, err := json.Marshal(production)
	if err != nil {
		fmt.Println("marshall json error:", err)
		panic("failed to marshall file " + production_file)
	}
     
    err = ioutil.WriteFile(strings.Replace(production_file,"csv","json",1), b, 0644)
    if err != nil {
		fmt.Println("dump json file error:", err)
        panic("failed to write json file " + production_file)
    }
    return nil
}

