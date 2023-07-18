//* Author : HARSH PATEL

package sync_es

import (
        "bytes"
        "context"
        "database/sql"
        "encoding/json"
        "errors"
        "fmt"
        "io/ioutil"
        "log"
        "os"
        "reflect"
        "strconv"
        "strings"
        "time"

        "github.com/BurntSushi/toml"
        "github.com/olivere/elastic"

        logger "../lib"
        _ "github.com/denisenkom/go-mssqldb"
)

type Config struct {
        Database   database
        elastisearchDB  elastisearchDB
        goConfig goConfig
}

type goConfig struct {
        maxLimit   string
        timeOut string
        ToRecord      string
}

type database struct {
        Server   string
        Port     string
        Database string
        User     string
        Password string
}

type elastisearchDB struct {
        elastisearchServer     string
        elastisearchPort string
        elastisearchUsername   string
        elastisearchPassword   string
        elastisearchIndex           string
}

type appconfig struct {
        maxLimit   string
        timeOut string
}

//SQLData data
type SQLData struct {
        ID               int    `json:"id"`
        data             int    `json:"data"`
}

//elasticData data
type elasticData struct {
        ID        int    `json:"ID"`
        data      int   `json:"data"`
}

//GetESClient ES Connection
func GetESClient() (*elastic.Client, error) {
        var conf Config
        if _, err := toml.DecodeFile("./config.toml", &conf); err != nil {
                fmt.Println(err)
        }
        //fmt.Println(conf)

        client, err := elastic.NewClient(elastic.SetURL("http://"+conf.elastisearchDB.elastisearchServer+":"+conf.elastisearchDB.elastisearchPort),
                elastic.SetSniff(false),
                elastic.SetHealthcheck(false), elastic.SetBasicAuth(conf.elastisearchDB.elastisearchUsername, conf.elastisearchDB.elastisearchPassword))
        fmt.Println("ELastic-Search Connection Sucessfully!")
        logger.InfoLogger.Println("ELastic-Search Connection Sucessfully!")
        return client, err
}

//ExecuteQuery sql
func ExecuteQuery(db *sql.DB, SQLQry string, params ...interface{}) (retVal []map[string]interface{}, err error) {
        defer func() {
                if r := recover(); r != nil {
                        retVal = nil
                        err = errors.New("Error In SQL Server")
                        log.Println("Error In SQL Server : ", r)
                        logger.ErrorLogger.Println("Error In SQL Server : ", r)
                }
        }()

        rows, Qerr := db.Query(SQLQry, params...)
        if Qerr != nil {
                return nil, Qerr
        }

        columns, _ := rows.Columns()

        count := len(columns)
        values := make([]interface{}, count)

        valuePtrs := make([]interface{}, count)

        rowCnt := 0
        for rows.Next() {
                retObj := make(map[string]interface{}, count)

                for i := range columns {
                        valuePtrs[i] = &values[i]
                }

                rows.Scan(valuePtrs...)

                for i, col := range columns {

                        var v interface{}

                        val := values[i]

                        b, ok := val.([]byte)

                        if ok {
                                v = string(b)
                        } else {
                                v = val
                        }
                        retObj[col] = v
                }
                retVal = append(retVal, retObj)
                rowCnt = rowCnt + 1
        }

        return retVal, err
}

//FormatFloat Date
func FormatFloat(num float64, prc int) string {
        var (
                zero, dot = "0", "."

                str = fmt.Sprintf("%."+strconv.Itoa(prc)+"f", num)
        )

        return strings.TrimRight(strings.TrimRight(str, zero), dot)
}

//ISODate struct
type ISODate struct {
        Format string
        time.Time
}

//UnmarshalJSON ISODate method
func (Date *ISODate) UnmarshalJSON(b []byte) error {
        var s string
        if err := json.Unmarshal(b, &s); err != nil {
                return err
        }
        Date.Format = "2022-01-24"
        t, _ := time.Parse(Date.Format, s)
        Date.Time = t
        return nil
}

// MarshalJSON ISODate method
func (Date ISODate) MarshalJSON() ([]byte, error) {
        return json.Marshal(Date.Time.Format(Date.Format))
}

func CheckESHealth(es *elastic.Client) {
        /*
                Get health stats using the driver's NewClusterHealthService
        */
        // Declare a elastic.ClusterHealthService object
        healthService := elastic.NewClusterHealthService(es)

        ctx := context.Background()
        // Call Do() to return a elastic.ClusterHealthResponse
        _, err := healthService.Do(ctx)

        // Check for health service errors
        if err != nil {
                fmt.Println("healthService.Do() ERROR: %v", err)
        }

        if err != nil {
                logger.ErrorLogger.Println("Error While connecting ElasticSearch!", err)
                fmt.Println(os.Interrupt)
                os.Exit(1)
        }
}

//StoneESSyc Stone data Sync
func StoneESSyc(db *sql.DB, es *elastic.Client) {
        var err error

        if dbErr := db.Ping(); dbErr != nil {
                logger.ErrorLogger.Println("SQL DB Connection Error.")
                logger.ErrorLogger.Println(dbErr)
                fmt.Println(os.Interrupt)
                os.Exit(1)
        }

        CheckESHealth(es)

        data, err := ioutil.ReadFile("offset.txt")
        if err != nil {
                fmt.Println(err)
                logger.ErrorLogger.Println("Exception Log : ", err)
        }

        offSet := string(data)
        offSet = strings.TrimSuffix(offSet, "\n")
        offSet = strings.TrimSuffix(offSet, "/r")

        log.Println("GO Start and Running!")
        logger.InfoLogger.Println("GO SYNC Start AT : ", offSet)

        var conf Config
        if _, err := toml.DecodeFile("./config.toml", &conf); err != nil {
                fmt.Println(err)
        }

        MaxLimitRecTmp := conf.goConfig.maxLimit
        maxLimit, err := strconv.Atoi(MaxLimitRecTmp)
        if err != nil {
                logger.ErrorLogger.Println("Error : ", err)
        }

        ToRecordTmp := conf.goConfig.ToRecord
        ToRecord, err := strconv.Atoi(ToRecordTmp)
        if err != nil {
                logger.ErrorLogger.Println("Error : ", err)
        }
        ToRecordOffSet, err := strconv.Atoi(offSet)
        if err != nil {
                logger.ErrorLogger.Println("Error : ", err)
        }
        ToRecord = ToRecordOffSet + ToRecord
        ToRecordstr := strconv.Itoa(ToRecord)

        //offSetLimit = offSetLimit + maxLimit
        newOffSetLimit := strconv.Itoa(maxLimit)
        //fmt.Print(string(data))
        logger.InfoLogger.Println("Previos Offset => ", offSet)
        logger.InfoLogger.Println("Max Limit => ", newOffSetLimit)

        var executeStatement, executeStatement1, executeStatement2 string
        executeStatement = ` id ,
                        [data]
                        FROM [dbo].[{YOUR VIEW NAME}]
                        WHERE ID > `
                executeStatement1 = ` SELECT TOP `
                executeStatement2 = ` ORDER BY ID `
        Query := executeStatement1 + newOffSetLimit + executeStatement + offSet + " AND ID < " + ToRecordstr + executeStatement2
        log.Println("SQL - Query => ", Query)
        var qryBuffer bytes.Buffer
        qryBuffer.WriteString(Query)
        start := time.Now()
        posts, err := ExecuteQuery(db, qryBuffer.String())
        if err != nil {
                //fmt.Println(err)
                logger.ErrorLogger.Println("Exception Log : ", err)
        }
        QueryExeTime := time.Since(start)
        logger.InfoLogger.Println("Query Execution tooks :: ", QueryExeTime.Seconds())
        fmt.Println("Query Execution Took :: ", QueryExeTime.Seconds())

        if len(posts) > 0 {
                ctx := context.Background()

                p, err := es.BulkProcessor().Name("MyBackgroundWorker-1").Workers(5).BulkActions(1000).FlushInterval(30 * time.Second).Do(context.Background())
                // Ask workers to commit all requests
                err = p.Flush()
                if err != nil {
                        logger.ErrorLogger.Println("Exception Log : ", err)
                        panic("Client fail ")
                }
                bulkRequest := es.Bulk()
                var lastoffset int
                for i := 0; i < len(posts); i++ {
                        sd := posts[i]
                        // convert map to json
                        jsonString, _ := json.Marshal(sd)

                        // convert json to struct
                        s := SQLData{}
                        json.Unmarshal(jsonString, &s)
                        logger.InfoLogger.Println("ID = ", s.ID)
                        logger.InfoLogger.Println("DATA = ", s.data)
                        logger.InfoLogger.Println("---------------------------------------------------------------")
                        var DataSet = elasticData{ID: s.ID, data: s.data}
                        req := elastic.NewBulkUpdateRequest().Index(conf.elastisearchDB.elastisearchIndex).Type("_doc").Id(strconv.Itoa(int(s.ID))).Doc(DataSet).DocAsUpsert(true) // s.id means which id do you want to upsert.
                        bulkRequest = bulkRequest.Add(req)
                        lastoffset = s.ID
                }

                //DocAsUpsert data
                bulkResponse, err := bulkRequest.Do(ctx)

                // Setup a bulk processor

                if err != nil {
                        //fmt.Println(err)
                        logger.ErrorLogger.Println("Exception Log : ", err)
                        logger.ErrorLogger.Println("Exception Log : ", bulkResponse.Items)
                }

                if bulkResponse.Failed() != nil {

                        //body, err := ioutil.ReadAll(bulkResponse.Failed())
                        //json.NewDecoder().Decode(&mapResp)
                        dataJSON, _ := json.Marshal(bulkResponse.Failed())
                        //fmt.Println(dataJSON)
                        js := string(dataJSON)
                        //fmt.Println(js)
                        logger.ErrorLogger.Println("------------------------------------------------------------------")
                        logger.ErrorLogger.Println("Exception Log total failed items = : ", len(bulkResponse.Failed()))
                        logger.ErrorLogger.Println("Exception Log failed items = : ", js)
                        logger.ErrorLogger.Println("------------------------------------------------------------------")

                }

                if bulkResponse.Succeeded() != nil {
                        mydata := []byte(strconv.Itoa(lastoffset))
                        err := ioutil.WriteFile("offset.txt", mydata, 0777) /// 0777 means chmod
                        if err != nil {
                                logger.ErrorLogger.Println("Exception Log : ", err)
                        }

                        logger.InfoLogger.Println("Elasticsearch Doccument Upsert Sucessfully!!")
                }
                p.Close()
                fmt.Println("Elasticsearch Doccument Upsert Sucessfully!!")

        } else {
                logger.InfoLogger.Println("No More Record...")
        }

}
func isNil(i interface{}) bool {
        if i == nil {
                return true
        }
        switch reflect.TypeOf(i).Kind() {
        case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
                return reflect.ValueOf(i).IsNil()
        }
        return false
}