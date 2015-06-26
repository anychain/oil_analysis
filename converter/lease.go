package main

import (
    "bufio"
    "encoding/csv"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strconv"
    "strings"
)

/*
CSV reader will ignore the header
*/

type LeaseDetail struct {
    Date          string
    MCFProduction int
    MCFDis        int
    BBLProduction int
    BBLDis        int
    OperName      string
    OperNo        int
    FieldName     string
    FieldNo       int
    LeaseNo       string
    LeaseName     string
    WellType      string
    District      string
}
type Lease struct {
    LeaseNo     string
    LeaseName   string
    WellType    string
    District    string
    LeaseDetail []LeaseDetail
}

func handleLease(output, lease_file string) error {

    fmt.Printf("Begin to deal with lease file %s \n", lease_file)
    file, err := os.Open(lease_file)
    var lease = new(Lease)
    if err != nil {
        fmt.Printf("failed to open file %s", lease_file)
        return nil
    }
    defer file.Close()
    header_reader := bufio.NewReader(file)
    for {
        line, err := header_reader.ReadString('\n')

        if err == io.EOF {
            fmt.Println("get the end of file ")
            break
        } else if err == nil {
            if strings.Index(line, "Lease Name:") == 1 {
                pos_lease_no := strings.Index(line, "Lease No.:")
                lease.LeaseName = line[14:pos_lease_no]
                lease.LeaseNo = line[pos_lease_no+12 : strings.LastIndex(line, "\"")]
            } else if strings.Index(line, "Well Type:") == 1 {
                lease.WellType = strings.TrimSpace(line[11:strings.LastIndex(line, "\"")])
            } else if strings.Index(line, "District:") == 1 {
                lease.District = line[10:strings.LastIndex(line, "\"")]
                //go back to the head
                file.Seek(0, 0)
                break
            }
        }
    }

    lease.LeaseDetail = make([]LeaseDetail, 0)

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
                tmp := new(LeaseDetail)
                tmp.Date = strings.TrimSpace(record[0])
                tmp.LeaseName = lease.LeaseName
                tmp.LeaseNo = strings.TrimSpace(lease.LeaseNo)
                tmp.District = strings.TrimSpace(lease.District)
                tmp.WellType = lease.WellType
                if lease.WellType == "Oil" {
                    tmp.BBLProduction, _ = strconv.Atoi(strings.Replace(record[1], ",", "", -1))
                    tmp.BBLDis, _ = strconv.Atoi(strings.Replace(record[2], ",", "", -1))
                    tmp.MCFProduction, _ = strconv.Atoi(strings.Replace(record[3], ",", "", -1))
                    tmp.MCFDis, _ = strconv.Atoi(strings.Replace(record[4], ",", "", -1))
                } else if lease.WellType == "Gas" {
                    tmp.MCFProduction, _ = strconv.Atoi(strings.Replace(record[1], ",", "", -1))
                    tmp.MCFDis, _ = strconv.Atoi(strings.Replace(record[2], ",", "", -1))
                    tmp.BBLProduction, _ = strconv.Atoi(strings.Replace(record[3], ",", "", -1))
                    tmp.BBLDis, _ = strconv.Atoi(strings.Replace(record[4], ",", "", -1))
                }
                if len(record) > 5 {
                    tmp.OperName = record[5]
                    tmp.OperNo, _ = strconv.Atoi(record[6])
                    tmp.FieldName = strings.Replace(record[7], "\"", "", -1)
                    tmp.FieldNo, _ = strconv.Atoi(record[8])
                }
                lease.LeaseDetail = append(lease.LeaseDetail, *tmp)
            }
        }

    }

    filename := output + string(filepath.Separator) + strings.Replace(filepath.Base(file.Name()), "csv", "json", 1)
    save_json(filename, lease.LeaseDetail)
    return nil
}
