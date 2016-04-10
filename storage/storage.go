package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pmalek/nlog"
)

type Handler struct{}

var log *nlog.Logger

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

	h := &Handler{}
	r := mux.NewRouter()
	r.HandleFunc("/sendImage", h.ReceiveImage).Methods(http.MethodPost)
	r.HandleFunc("/getImage", h.ServeImage).Methods(http.MethodGet)

	log.Infof("Starting storage server at :3002 ...")
	http.ListenAndServe(":3002", r)
}

func (h *Handler) ReceiveImage(w http.ResponseWriter, r *http.Request) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("Problem parsing URL query", nlog.Data{"values": values, "r.URL": r.URL})
		return
	}

	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "No id in GET attributes")
		log.Error("No id in GET attributes", nlog.Data{"values": values})
		return
	}

	state := values.Get("state")
	if state != "working" && state != "finished" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Wrong input state.")
		log.Error("", nlog.Data{"values": values})
		return
	}

	id := values.Get("id")
	_, err = strconv.Atoi(id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Wrong input id.")
		log.Error("Cannot parse id as a number", nlog.Data{"err": err, "id": id})
		return
	}

	file, err := os.Create("/tmp/" + state + "/" + id + ".png")
	defer file.Close()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err})
		return
	}

	_, err = io.Copy(file, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err})
		return
	}

	fmt.Fprint(w, "success")
}

func (h *Handler) ServeImage(w http.ResponseWriter, r *http.Request) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err})
		return
	}

	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Wrong input id.")
		log.Error("", nlog.Data{"values": values})
		return
	}

	if values.Get("state") != "working" && values.Get("state") != "finished" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Wrong input state.")
		log.Error("", nlog.Data{"values": values})
		return
	}

	_, err = strconv.Atoi(values.Get("id"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Wrong input id.")
		log.Error("", nlog.Data{"err": err})
		return
	}

	file, err := os.Open("/tmp/" + values.Get("state") + "/" + values.Get("id") + ".png")
	defer file.Close()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("", nlog.Data{"err": err})
		return
	}

	_, err = io.Copy(w, file)
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
	storageAddress := os.Args[1] // The address of itself
	keyValueStoreAddress := os.Args[2]

	response, err := http.Post("http://"+keyValueStoreAddress+"/set?key=storageAddress&value="+storageAddress, "", nil)
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
