package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"github.com/gorilla/mux"
	"github.com/pmalek/nlog"
)

const indexPage = "<html><head><title>Upload file</title></head><body><form enctype=\"multipart/form-data\" action=\"submitTask\" method=\"post\"> <input type=\"file\" name=\"uploadfile\" /> <input type=\"submit\" value=\"upload\" /> </form> </body> </html>"

var (
	keyValueStoreAddress string
	masterLocation       string
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
	if len(os.Args) < 2 {
		fmt.Println("Error: Too few arguments.")
		return
	}
	keyValueStoreAddress = os.Args[1]

	response, err := http.Get("http://" + keyValueStoreAddress + "/get?key=masterAddress")
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: can't get master address.")
		fmt.Println(response.Body)
		return
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	masterLocation = string(data)
	if len(masterLocation) == 0 {
		err_str := "Error: can't get master address. Length is zero."
		fmt.Println(err_str)
		log.Errorf(err_str)
		return
	}

	r := mux.NewRouter()
	r.HandleFunc("/", handleIndex)
	r.HandleFunc("/submitTask", handleTask).Methods(http.MethodPost)
	r.HandleFunc("/isReady", handleCheckForReadiness).Methods(http.MethodGet)
	r.HandleFunc("/getImage", serveImage).Methods(http.MethodGet)

	log.Infof("Starting frontend at :8000 ...")
	http.ListenAndServe(":8000", r)
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	log.Infof("")
	fmt.Fprint(w, indexPage)
}

func handleTask(w http.ResponseWriter, r *http.Request) {
	log.Infof("")
	err := r.ParseMultipartForm(10000000)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Wrong input")
		log.Errorf("Wrong input")
		return
	}

	file, _, err := r.FormFile("uploadfile")

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Wrong input")
		log.Errorf("Wrong input")
		return
	}

	response, err := http.Post("http://"+masterLocation+"/new", "image", file)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Errorf("Wrong input", nlog.Data{"err": err, "response.StatusCode": response.StatusCode})
		return
	} else if response.StatusCode != http.StatusOK {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Errorf("Wrong input", nlog.Data{"response.StatusCode": response.StatusCode})
		return
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}

	fmt.Fprint(w, string(data))
}

func handleCheckForReadiness(w http.ResponseWriter, r *http.Request) {
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

	response, err := http.Get("http://" + masterLocation + "/isReady?id=" + values.Get("id") + "&state=finished")
	if err != nil || response.StatusCode != http.StatusOK {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		return
	}

	switch string(data) {
	case "0":
		fmt.Fprint(w, "Your image is not ready yet.")
	case "1":
		fmt.Fprint(w, "Your image is ready.")
	default:
		fmt.Fprint(w, "Internal server error.")
	}
}

func serveImage(w http.ResponseWriter, r *http.Request) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		fmt.Fprint(w, err)
		return
	}
	if len(values.Get("id")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Wrong input")
		log.Errorf("Wrong input: zero 'id's in request")
		return
	}

	response, err := http.Get("http://" + masterLocation + "/get?id=" + values.Get("id") + "&state=finished")
	if err != nil || response.StatusCode != http.StatusOK {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("Wrong input", nlog.Data{"err": err})
		return
	}

	_, err = io.Copy(w, response.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("Wrong input", nlog.Data{"err": err})
		return
	}
}
