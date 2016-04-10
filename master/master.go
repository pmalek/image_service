package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pmalek/image_service/task"
	"github.com/pmalek/nlog"
)

type Handler struct {
	client *rpc.Client
}

var (
	databaseLocation     string
	keyValueStoreAddress string
	storageLocation      string
	log                  *nlog.Logger
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

func main() {
	if !registerInKVStore() {
		return
	}

	keyValueStoreAddress = os.Args[2]
	if ok := getSlavesAddressesFromDatabase(); ok == false {
		return
	}

	h := &Handler{}
	h.initializeWorkerConn()
	defer h.client.Close()
	r := mux.NewRouter()
	r.HandleFunc("/new", h.NewImage).Methods(http.MethodPost)
	r.HandleFunc("/get", h.GetImage)
	r.HandleFunc("/isReady", h.IsReady)
	r.HandleFunc("/getNewTask", h.GetNewTask)
	r.HandleFunc("/registerTaskFinished", h.RegisterTaskFinished)

	log.Infof("Starting master server at :3003 ...")
	http.ListenAndServe(":3003", r)
}

func (h *Handler) NewImage(w http.ResponseWriter, r *http.Request) {
	response, err := http.Post("http://"+databaseLocation+"/newTask", "text/plain", nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err})
		return
	}

	id, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		log.Error("", nlog.Data{"err": err})
		return
	}
	id_str := string(id)
	id_int, err := strconv.Atoi(id_str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Error("id is not a parsable int", nlog.Data{"err": err, "id_str": id_str})
		return
	}

	_, err = http.Post("http://"+storageLocation+"/sendImage?id="+id_str+"&state=working", "image", r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err, "id_str": id_str})
		return
	}
	fmt.Fprint(w, id_str)

	log.Info("Notifying workers that there is task to be done", nlog.Data{"id_int": id_int})
	var result int
	go h.client.Call("Notifier.Notify", id_int, &result)
}

func (h *Handler) GetImage(w http.ResponseWriter, r *http.Request) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		fmt.Fprint(w, err)
		return
	}
	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Wrong input")
		return
	}

	response, err := http.Get("http://" + storageLocation + "/getImage?id=" + values.Get("id") + "&state=finished")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}

	_, err = io.Copy(w, response.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err})
		return
	}
}

func (h *Handler) IsReady(w http.ResponseWriter, r *http.Request) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		fmt.Fprint(w, err)
		return
	}
	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Wrong input")
		return
	}

	response, err := http.Get("http://" + databaseLocation + "/getById?id=" + values.Get("id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	myTask := task.Task{}
	json.Unmarshal(data, &myTask)

	if myTask.State == 2 {
		fmt.Fprint(w, "1")
	} else {
		fmt.Fprint(w, "0")
	}
}

func (h *Handler) GetNewTask(w http.ResponseWriter, r *http.Request) {
	response, err := http.Post("http://"+databaseLocation+"/getNewTask", "text/plain", nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err})
		return
	}

	if response.StatusCode == http.StatusNoContent {
		log.Info("", nlog.Data{"response.StatusCode ": response.StatusCode})
		w.WriteHeader(http.StatusNoContent)
		return
	}

	_, err = io.Copy(w, response.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err})
		return
	}
}

func (h *Handler) RegisterTaskFinished(w http.ResponseWriter, r *http.Request) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		fmt.Fprint(w, err)
		return
	}
	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Wrong input")
		return
	}

	response, err := http.Post("http://"+databaseLocation+"/finishTask?id="+values.Get("id"), "test/plain", nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}

	_, err = io.Copy(w, response.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err})
		return
	}
}

func registerInKVStore() bool {
	if len(os.Args) < 3 {
		fmt.Println("Error: Too few arguments.")
		return false
	}
	masterAddress := os.Args[1] // The address of itself
	keyValueStoreAddress := os.Args[2]

	response, err := http.Post("http://"+keyValueStoreAddress+"/set?key=masterAddress&value="+masterAddress, "", nil)
	if err != nil {
		fmt.Println(err)
		return false
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return false
	}
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: Failure when contacting key-value store: ", string(data))
		return false
	}
	return true
}

func getSlavesAddressesFromDatabase() bool {
	response, err := http.Get("http://" + keyValueStoreAddress + "/get?key=databaseAddress")
	if err != nil {
		log.Error("Couldn't get database address", nlog.Data{"err": err})
		return false
	} else if response.StatusCode != http.StatusOK {
		fmt.Println("Error: can't get database address.")
		fmt.Println(response.Body)
		return false
	}

	if data, err := ioutil.ReadAll(response.Body); err != nil {
		fmt.Println(err)
		log.Error("Couldn't read database address", nlog.Data{"err": err, "data": data})
		return false
	} else {
		databaseLocation = string(data)
	}

	if len(databaseLocation) == 0 {
		log.Errorf("databaseLocation empty (set its address in key value store)")
		return false
	}

	response, err = http.Get("http://" + keyValueStoreAddress + "/get?key=storageAddress")
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: can't get storage address.")
		fmt.Println(response.Body)
		return false
	}

	if data, err := ioutil.ReadAll(response.Body); err != nil {
		fmt.Println(err)
		return false
	} else {
		storageLocation = string(data)
	}

	if len(storageLocation) == 0 {
		log.Errorf("storageLocation empty (set its address in key value store)")
		return false
	}

	return true
}

func (h *Handler) initializeWorkerConn() {
	var err error
	h.client, err = rpc.DialHTTP("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("dialing:", nlog.Data{"err": err})
	}
}
