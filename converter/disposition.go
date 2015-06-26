package main

import (
    "bufio"
    "encoding/csv"
    "encoding/json"
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "strconv"
    "strings"
)

/*
CSV reader will ignore the header
*/

type Disposition struct {
    LeaseNo         string
    LeaseName       string
    WellType        string
    District        string
    DisposDetailOil []DisposOil
    DisposDetailGas []DisposGas
}

type DisposOil struct {
    LeaseNo       string
    LeaseName     string
    WellType      string
    District      string
    Date          string
    Production    int
    OilDis0       int
    OilDis1       int
    OilDis2       int
    OilDis3       int
    OilDis4       int
    OilDis5       int
    OilDis6       int
    OilDis7       int
    OilDis8       int
    OilDis9       int
    OilDisNc      int
    GasProduction int
    GasDis1       int
    GasDis2       int
    GasDis3       int
    GasDis4       int
    GasDis5       int
    GasDis6       int
    GasDis7       int
    GasDis8       int
    GasDisNc      int
}

type DisposGas struct {
    LeaseNo       string
    LeaseName     string
    WellType      string
    District      string
    Date          string
    Production    int
    GasDis1       int
    GasDis2       int
    GasDis3       int
    GasDis4       int
    GasDis5       int
    GasDis6       int
    GasDis7       int
    GasDis8       int
    GasDis9       int
    GasDisNc      int
    OilProduction int
    OilDis0       int
    OilDis1       int
    OilDis2       int
    OilDis3       int
    OilDis4       int
    OilDis5       int
    OilDis6       int
    OilDis7       int
    OilDis8       int
    OilDisNc      int
}

func handleDisposition(disposition_file string) error {

    fmt.Printf("Begin to deal with disposition file %s \n", disposition_file)
    file, err := os.Open(disposition_file)
    if err != nil {
        fmt.Printf("failed to open file %s", disposition_file)
        return nil
    }
    defer file.Close()
    header_reader := bufio.NewReader(file)
    var dispos = new(Disposition)
    for {
        line, err := header_reader.ReadString('\n')

        if err == io.EOF {
            fmt.Println("get the end of file ")
            break
        } else if err == nil {
            if strings.Index(line, "Lease Name:") == 1 {
                pos_lease_no := strings.Index(line, "Lease No.:")
                dispos.LeaseName = line[14:pos_lease_no]
                dispos.LeaseNo = line[pos_lease_no+12 : strings.LastIndex(line, "\"")]
            } else if strings.Index(line, "Well Type:") == 1 {
                dispos.WellType = strings.TrimSpace(line[11:strings.LastIndex(line, "\"")])
            } else if strings.Index(line, "District:") == 1 {
                dispos.District = line[10:strings.LastIndex(line, "\"")]
                //go back to the head
                file.Seek(0, 0)
                break
            }
        }
    }

    if dispos.WellType == "Gas" {
        dispos.DisposDetailOil = make([]DisposOil, 0)
    } else if dispos.WellType == "Oil" {
        dispos.DisposDetailGas = make([]DisposGas, 1)
    } else {
        panic("wrong well type" + dispos.WellType)
    }

    reader := csv.NewReader(file)
    for {

        record, err := reader.Read()
        if err == io.EOF {
            break
        } else if err != nil {
            //skip the string with empty first element
            if len(strings.Trim(record[0], " ")) == 0 {
            } else if record[0] == "Date" {
            } else if record[0] == "Total" {
                //skip the total line
            } else {
                if dispos.WellType == "Oil" {
                    tmp := new(DisposOil)
                    tmp.LeaseName = dispos.LeaseName
                    tmp.LeaseNo = strings.TrimSpace(dispos.LeaseNo)
                    tmp.WellType = dispos.WellType
                    tmp.District = strings.TrimSpace(dispos.District)
                    tmp.Date = strings.TrimSpace(record[0])
                    tmp.Production, _ = strconv.Atoi(strings.Replace(record[1], ",", "", -1))
                    tmp.OilDis0, _ = strconv.Atoi(strings.Replace(record[2], ",", "", -1))
                    tmp.OilDis1, _ = strconv.Atoi(strings.Replace(record[3], ",", "", -1))
                    tmp.OilDis2, _ = strconv.Atoi(strings.Replace(record[4], ",", "", -1))
                    tmp.OilDis3, _ = strconv.Atoi(strings.Replace(record[5], ",", "", -1))
                    tmp.OilDis4, _ = strconv.Atoi(strings.Replace(record[6], ",", "", -1))
                    tmp.OilDis5, _ = strconv.Atoi(strings.Replace(record[7], ",", "", -1))
                    tmp.OilDis6, _ = strconv.Atoi(strings.Replace(record[8], ",", "", -1))
                    tmp.OilDis7, _ = strconv.Atoi(strings.Replace(record[9], ",", "", -1))
                    tmp.OilDis8, _ = strconv.Atoi(strings.Replace(record[10], ",", "", -1))
                    tmp.OilDis9, _ = strconv.Atoi(strings.Replace(record[11], ",", "", -1))
                    tmp.OilDisNc, _ = strconv.Atoi(strings.Replace(record[12], ",", "", -1))
                    tmp.GasProduction, _ = strconv.Atoi(strings.Replace(record[13], ",", "", -1))
                    tmp.GasDis1, _ = strconv.Atoi(strings.Replace(record[14], ",", "", -1))
                    tmp.GasDis2, _ = strconv.Atoi(strings.Replace(record[15], ",", "", -1))
                    tmp.GasDis3, _ = strconv.Atoi(strings.Replace(record[16], ",", "", -1))
                    tmp.GasDis4, _ = strconv.Atoi(strings.Replace(record[17], ",", "", -1))
                    tmp.GasDis5, _ = strconv.Atoi(strings.Replace(record[18], ",", "", -1))
                    tmp.GasDis6, _ = strconv.Atoi(strings.Replace(record[19], ",", "", -1))
                    tmp.GasDis7, _ = strconv.Atoi(strings.Replace(record[20], ",", "", -1))
                    tmp.GasDis8, _ = strconv.Atoi(strings.Replace(record[21], ",", "", -1))
                    tmp.GasDisNc, _ = strconv.Atoi(strings.Replace(record[22], ",", "", -1))
                    dispos.DisposDetailOil = append(dispos.DisposDetailOil, *tmp)
                } else if dispos.WellType == "Gas" {
                    tmp := new(DisposGas)
                    tmp.LeaseName = dispos.LeaseName
                    tmp.LeaseNo = strings.TrimSpace(dispos.LeaseNo)
                    tmp.WellType = dispos.WellType
                    tmp.District = strings.TrimSpace(dispos.District)
                    tmp.Date = strings.TrimSpace(record[0])
                    tmp.Production, _ = strconv.Atoi(strings.Replace(record[1], ",", "", -1))
                    tmp.GasDis1, _ = strconv.Atoi(strings.Replace(record[2], ",", "", -1))
                    tmp.GasDis2, _ = strconv.Atoi(strings.Replace(record[3], ",", "", -1))
                    tmp.GasDis3, _ = strconv.Atoi(strings.Replace(record[4], ",", "", -1))
                    tmp.GasDis4, _ = strconv.Atoi(strings.Replace(record[5], ",", "", -1))
                    tmp.GasDis5, _ = strconv.Atoi(strings.Replace(record[6], ",", "", -1))
                    tmp.GasDis6, _ = strconv.Atoi(strings.Replace(record[7], ",", "", -1))
                    tmp.GasDis7, _ = strconv.Atoi(strings.Replace(record[8], ",", "", -1))
                    tmp.GasDis8, _ = strconv.Atoi(strings.Replace(record[9], ",", "", -1))
                    tmp.GasDis9, _ = strconv.Atoi(strings.Replace(record[10], ",", "", -1))
                    tmp.GasDisNc, _ = strconv.Atoi(strings.Replace(record[11], ",", "", -1))
                    tmp.OilProduction, _ = strconv.Atoi(strings.Replace(record[12], ",", "", -1))
                    tmp.OilDis0, _ = strconv.Atoi(strings.Replace(record[13], ",", "", -1))
                    tmp.OilDis1, _ = strconv.Atoi(strings.Replace(record[14], ",", "", -1))
                    tmp.OilDis2, _ = strconv.Atoi(strings.Replace(record[15], ",", "", -1))
                    tmp.OilDis3, _ = strconv.Atoi(strings.Replace(record[16], ",", "", -1))
                    tmp.OilDis4, _ = strconv.Atoi(strings.Replace(record[17], ",", "", -1))
                    tmp.OilDis5, _ = strconv.Atoi(strings.Replace(record[18], ",", "", -1))
                    tmp.OilDis6, _ = strconv.Atoi(strings.Replace(record[19], ",", "", -1))
                    tmp.OilDis7, _ = strconv.Atoi(strings.Replace(record[20], ",", "", -1))
                    tmp.OilDis8, _ = strconv.Atoi(strings.Replace(record[21], ",", "", -1))
                    tmp.OilDisNc, _ = strconv.Atoi(strings.Replace(record[22], ",", "", -1))
                    dispos.DisposDetailGas = append(dispos.DisposDetailGas, *tmp)
                }
            }
        }

    }

    if len(dispos.DisposDetailOil) > 0 {
        oil, err := json.Marshal(dispos.DisposDetailOil)
        if err != nil {
            fmt.Println("marshall json error:", err)
            panic("failed to marshall file " + disposition_file)
        }

        err = ioutil.WriteFile(strings.Replace(disposition_file, "csv", "oil.json", 1), oil, 0644)
        if err != nil {
            fmt.Println("dump json file error:", err)
            panic("failed to write json file " + disposition_file)
        }
    }

    if len(dispos.DisposDetailGas) > 0 {
        gas, err := json.Marshal(dispos.DisposDetailGas)
        if err != nil {
            fmt.Println("marshall json error:", err)
            panic("failed to marshall file " + disposition_file)
        }

        err = ioutil.WriteFile(strings.Replace(disposition_file, "csv", "gas.json", 1), gas, 0644)
        if err != nil {
            fmt.Println("dump json file error:", err)
            panic("failed to write json file " + disposition_file)
        }
    }
    return nil
}
