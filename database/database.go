package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
	"github.com/pmalek/image_service/task"
	"github.com/pmalek/nlog"
)

var (
	datastore      map[int]task.Task
	datastoreMutex sync.RWMutex
	oNFTMutex      sync.RWMutex

	// remember to account for potential int overflow in production. Use something bigger.
	oldestNotFinishedTask int

	log *nlog.Logger
)

func init() {
	formatter := nlog.NewTextFormatter(true, true)
	formatter.TimestampFormat = "2006-04-02 15:04:05.000000 -0700"
	log = nlog.NewLogger(nlog.InfoLevel, formatter)

	file, err := os.OpenFile("log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Failed to open log file", nlog.Data{"err": err})
	}
	multi := io.MultiWriter(file, os.Stdout)
	log.SetOut(multi)
}

func registerInKVStore() bool {
	if len(os.Args) < 3 {
		fmt.Println("Error: Too few arguments.")
		return false
	}

	databaseAddress := os.Args[1] // The address of itself
	keyValueStoreAddress := os.Args[2]

	response, err := http.Post("http://"+keyValueStoreAddress+"/set?key=databaseAddress&value="+databaseAddress, "", nil)
	if err != nil {
		log.Info("Couldn't get database address from key value store", nlog.Data{"err": err})
		return false
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Info("", nlog.Data{"err": err})
		return false
	}
	if response.StatusCode != http.StatusOK {
		log.Info("Error: Failure when contacting key-value store: ", nlog.Data{"data": data})
		return false
	}
	return true
}

func init() {
	datastore = make(map[int]task.Task)
	datastoreMutex = sync.RWMutex{}
	oldestNotFinishedTask = 0
	oNFTMutex = sync.RWMutex{}
}

func main() {
	if !registerInKVStore() {
		return
	}

	h := NewHandler()
	r := mux.NewRouter()
	r.HandleFunc("/getById", h.GetById).Methods(http.MethodGet)
	r.HandleFunc("/newTask", h.NewTask).Methods(http.MethodPost)
	r.HandleFunc("/getNewTask", h.GetNewTask).Methods(http.MethodPost)
	r.HandleFunc("/finishTask", h.FinishTask).Methods(http.MethodPost)
	r.HandleFunc("/setById", h.SetById).Methods(http.MethodPost)
	r.HandleFunc("/list", h.List).Methods(http.MethodGet)

	log.Infof("Starting database server at :3001...")
	http.ListenAndServe(":3001", r)
}
