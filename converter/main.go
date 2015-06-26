package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "runtime/debug"
)

var data_root_path = flag.String("root", ".", "The root path of csv files")
var output_path = flag.String("output", ".", "The output path of json files")
var (
    prodDetail      = make([]ProductionDetail, 0)
    leaseDetail     = make([]LeaseDetail, 0)
    disposGasDetail = make([]DisposGas, 0)
    disposOilDetail = make([]DisposOil, 0)
)

func save_json(filename string, data interface{}) error {
    //TODO split data to avoid Too many bytes before newline error when load by spark
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

    // e := filepath.Walk(*data_root_path, listFiles)
    // if e != nil {
    //     fmt.Println("failed to parse the root folder")
    // } else {
    fmt.Println("Loading production json files")
    e := filepath.Walk(*output_path, loadProdFiles)
    if e != nil {
        fmt.Println("Failed to parse the " + *output_path)
    } else {
        if len(prodDetail) > 0 {
            fmt.Println("Saving production sum json")
            save_json(*output_path+string(filepath.Separator)+"sum_production.json", prodDetail)
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
            fmt.Println("Saving lease sum json")
            save_json(*output_path+string(filepath.Separator)+"sum_lease.json", leaseDetail)
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
            fmt.Println("Saving disposition gas json")
            save_json(*output_path+string(filepath.Separator)+"sum_disposition_gas.json", disposGasDetail)
            // release memory
            disposGasDetail = nil
            debug.FreeOSMemory()
        }
        if len(disposOilDetail) > 0 {
            fmt.Println("Saving disposition oil json")
            save_json(*output_path+string(filepath.Separator)+"sum_disposition_oil.json", disposOilDetail)
            // release memory
            disposOilDetail = nil
            debug.FreeOSMemory()
        }
    }
    // }

}
