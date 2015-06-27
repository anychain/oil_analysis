package main

import (
    "encoding/json"
    "flag"
    "fmt"
    csv_ex "github.com/jweir/csv"
    "io/ioutil"
    "os"
    "path/filepath"
    "runtime/debug"
)

const MaxInt = int(^uint(0) >> 1)

var data_root_path = flag.String("root", ".", "The root path of csv files")
var output_path = flag.String("output", ".", "The output path of csv files")
var (
    prodDetail      = make([]ProductionDetail, 0)
    leaseDetail     = make([]LeaseDetail, 0)
    disposGasDetail = make([]DisposGas, 0)
    disposOilDetail = make([]DisposOil, 0)
)

func save_csv(filename string, data interface{}) error {
    fmt.Println("Save to " + filename)
    b, err := csv_ex.Marshal(data)
    if err != nil {
        fmt.Println("marshall json error:", err)
        panic("failed to marshall file " + filename)
    }

    err = ioutil.WriteFile(filename, b, 0644)
    if err != nil {
        fmt.Println("dump json file error:", err)
        panic("failed to write json file " + filename)
    }
    return err
}

func save_json(filename string, data interface{}) error {
    //TODO split data to avoid Too many bytes before newline error when load by spark
    // data_type := reflect.TypeOf(data).Kind()
    // switch data_type {
    // case reflect.Slice:
    //     s := reflect.ValueOf(data)
    //     length := s.Len()
    //     if length > 0 {
    return _save_json(filename, data)
    // totalBytes := reflect.TypeOf(s[0].Interface()).Size() * length
    // if totalBytes > MaxInt {
    //     fmt.Println("Need split the data since it is too long: ", totalBytes)
    //     slice_num := uint64(math.Ceil(float64(totalBytes) / float64(MaxInt)))
    //     chunks(s, slice_num, filename)
    // } else {
    //     _save(filename, data)
    // }
    //     }
    // default:
    //     panic("Dose not support this type to save in json file: " + data_type)
    // }
}

// func chunks(s interface{}, number int, filename string) {
//     length := s.Len()
//     offset := int(math.Ceil(float64(length) / float64(number)))
//     if offset > 0 {
//         keep_offset_num := number - (number*offset - length)
//         i := 0
//         current_chunk := 0
//         for ; i < length; i += offset {
//             current_chunk++
//             if current_chunk > keep_offset_num {
//                 offset--
//                 current_chunk = 0
//                 offset = int(math.Ceil(float64(length-keep_offset_num) / float64(number)))
//                 keep_offset_num = number - (number*offset - length)
//             }
//             if offset == 0 {
//                 break
//             }
//             if i+offset <= length {
//                 filename = strings.Replace(filename, ".json", "_"+i+".json", 1)
//                 _save_json(filename, s[i:i+offset])
//                 // fmt.Println(i, s[i:i+offset])
//             }
//         }
//     } else {
//         panic("Unable to split the data since length of slice is too small: " + length + "," + number)
//     }
// }

func _save_json(filename string, data interface{}) error {
    fmt.Println("Save " + filename)
    b, err := json.Marshal(data)
    if err != nil {
        fmt.Println("marshall json error:", err)
        panic("failed to marshall file " + filename)
    }

    err = ioutil.WriteFile(filename, b, 0644)
    if err != nil {
        fmt.Println("dump json file error:", err)
        panic("failed to write json file " + filename)
    }
    return err

}

func loadDispoFiles(path string, f os.FileInfo, err error) error {
    if f == nil {
        return err
    }
    if f.IsDir() {
        return nil
    }
    file, err := os.Open(path)
    if err != nil {
        fmt.Printf("failed to open file %s", path)
        return nil
    }
    defer file.Close()
    matched, _ := filepath.Match("disposition*.gas.json", filepath.Base(path))
    if matched {
        var detail []DisposGas
        jsonParser := json.NewDecoder(file)
        if err = jsonParser.Decode(&detail); err != nil {
            fmt.Printf("Fail to parsing file %s : %s", f.Name, err.Error())
        } else {
            disposGasDetail = append(disposGasDetail, detail...)
        }
    }
    matched, _ = filepath.Match("disposition*.oil.json", filepath.Base(path))
    if matched {
        var detail []DisposOil
        jsonParser := json.NewDecoder(file)
        if err = jsonParser.Decode(&detail); err != nil {
            fmt.Printf("Fail to parsing file %s : %s", f.Name, err.Error())
        } else {
            disposOilDetail = append(disposOilDetail, detail...)
        }
    }
    return nil
}

func loadLeaseFiles(path string, f os.FileInfo, err error) error {
    if f == nil {
        return err
    }
    if f.IsDir() {
        return nil
    }
    file, err := os.Open(path)
    if err != nil {
        fmt.Printf("failed to open file %s", path)
        return nil
    }
    defer file.Close()
    matched, _ := filepath.Match("lease*.json", filepath.Base(path))
    if matched {
        var detail []LeaseDetail
        jsonParser := json.NewDecoder(file)
        if err = jsonParser.Decode(&detail); err != nil {
            fmt.Printf("Fail to parsing file %s : %s", f.Name, err.Error())
        } else {
            leaseDetail = append(leaseDetail, detail...)
        }
    }
    return nil
}

func loadProdFiles(path string, f os.FileInfo, err error) error {
    if f == nil {
        return err
    }
    if f.IsDir() {
        return nil
    }
    file, err := os.Open(path)
    if err != nil {
        fmt.Printf("failed to open file %s", path)
        return nil
    }
    defer file.Close()
    matched, _ := filepath.Match("production*.json", filepath.Base(path))
    if matched {
        var detail []ProductionDetail
        jsonParser := json.NewDecoder(file)
        if err = jsonParser.Decode(&detail); err != nil {
            fmt.Printf("Fail to parsing file %s : %s", f.Name, err.Error())
        } else {
            prodDetail = append(prodDetail, detail...)
        }
    }
    return nil
}

func listFiles(path string, f os.FileInfo, err error) error {
    if f == nil {
        return err
    }
    if f.IsDir() {
        return nil
    }
    matched, _ := filepath.Match("disposition*.csv", filepath.Base(path))
    if matched {
        e := handleDisposition(*output_path, path)
        if e != nil {
            fmt.Println("failed to deal with ", path)
        }
    }

    matched, _ = filepath.Match("lease*.csv", filepath.Base(path))
    if matched {
        e := handleLease(*output_path, path)
        if e != nil {
            fmt.Println("failed to deal with ", path)
        }
    }
    matched, _ = filepath.Match("production*.csv", filepath.Base(path))
    if matched {
        e := handleProduction(*output_path, path)

        if e != nil {
            fmt.Println("failed to deal with ", path)
        }
    }
    return nil
}

/*Parse the root folder to find out all csv files and
  convert according to predefined rules
*/
func main() {
    if len(os.Args) < 2 {
        fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
        flag.PrintDefaults()
        return
    }
    flag.Parse()

    fi, err := os.Stat(*data_root_path)
    if err == nil {
        if !fi.IsDir() {
            fmt.Printf(" %s is not a valid path\n", *data_root_path)
            return
        }
    } else {
        fmt.Printf(" failed to open %s , Error is \"%s\" \n", *data_root_path, err)
        return
    }

    e := filepath.Walk(*data_root_path, listFiles)
    if e != nil {
        fmt.Println("failed to parse the root folder")
    } else {
        fmt.Println("Loading production json files")
        e := filepath.Walk(*output_path, loadProdFiles)
        if e != nil {
            fmt.Println("Failed to parse the " + *output_path)
        } else {
            if len(prodDetail) > 0 {
                fmt.Println("Saving production sum csv")
                save_csv(*output_path+string(filepath.Separator)+"sum_production.csv", prodDetail)
                // release memory
                prodDetail = nil
                debug.FreeOSMemory()
            }
        }

        fmt.Println("Loading lease json files")
        e = filepath.Walk(*output_path, loadLeaseFiles)
        if e != nil {
            fmt.Println("Failed to parse the " + *output_path)
        } else {
            if len(leaseDetail) > 0 {
                fmt.Println("Saving lease sum csv")
                save_csv(*output_path+string(filepath.Separator)+"sum_lease.csv", leaseDetail)
                // release memory
                leaseDetail = nil
                debug.FreeOSMemory()
            }
        }

        fmt.Println("Loading disposition json files")
        e = filepath.Walk(*output_path, loadDispoFiles)
        if e != nil {
            fmt.Println("Failed to parse the " + *output_path)
        } else {
            if len(disposGasDetail) > 0 {
                fmt.Println("Saving disposition gas csv")
                save_csv(*output_path+string(filepath.Separator)+"sum_disposition_gas.csv", disposGasDetail)
                // release memory
                disposGasDetail = nil
                debug.FreeOSMemory()
            }
            if len(disposOilDetail) > 0 {
                fmt.Println("Saving disposition oil csv")
                save_csv(*output_path+string(filepath.Separator)+"sum_disposition_oil.csv", disposOilDetail)
                // release memory
                disposOilDetail = nil
                debug.FreeOSMemory()
            }
        }
    }

}
