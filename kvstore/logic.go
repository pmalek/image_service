package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/pmalek/nlog"
)

var (
	keyValueStore map[string]string
	kVStoreMutex  sync.RWMutex
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func init() {
	keyValueStore = make(map[string]string)
	kVStoreMutex = sync.RWMutex{}
}

//func get(w http.ResponseWriter, r *http.Request) {
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	log.Debug("get", nlog.Data{"r": r})

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("URL Parsing failed", nlog.Data{"err": err})
		return
	}

	k := values.Get("key")
	if len(k) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Wrong input key.")
		log.Error("No values at 'key' in GET message", nlog.Data{"k": k})
		return
	}

	kVStoreMutex.RLock()
	value, ok := keyValueStore[string(k)]
	kVStoreMutex.RUnlock()

	if !ok {
		log.Error("No value at key:", nlog.Data{"k": k})
	} else {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, value)
		log.Info("Successfully retrieved", nlog.Data{"k": k, "value": value})
	}
}

//func set(w http.ResponseWriter, r *http.Request) {
func (h *Handler) Set(w http.ResponseWriter, r *http.Request) {
	log.Debug("set", nlog.Data{"r": r})

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", err)
		log.Error("URL Parsing failed", nlog.Data{"err": err})
		return
	}

	if len(values.Get("key")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Wrong input key.")
		log.Error("No key sent", nlog.Data{"values": values})
		return
	}

	if len(values.Get("value")) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Wrong input value.")
		log.Error("No value sent", nlog.Data{"values": values})
		return
	}

	key := string(values.Get("key"))
	val := string(values.Get("value"))

	kVStoreMutex.Lock()
	keyValueStore[key] = val
	kVStoreMutex.Unlock()

	w.WriteHeader(http.StatusOK)
	log.Info("Successfully saved in key value store", nlog.Data{"key": key, "val": val})
}

//func remove(w http.ResponseWriter, r *http.Request) {
func (h *Handler) Remove(w http.ResponseWriter, r *http.Request) {
	log.Debug("remove", nlog.Data{"r": r})

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Error("", nlog.Data{"err": err})
		return
	}

	key := values.Get("key")
	if len(key) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		log.Error("Wrong input key", nlog.Data{"err": err})
		return
	}

	kVStoreMutex.Lock()
	delete(keyValueStore, key)
	kVStoreMutex.Unlock()

	w.WriteHeader(http.StatusOK)
	log.Info("Successfully deleted", nlog.Data{"key": key})
}

//func list(w http.ResponseWriter, r *http.Request) {
func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	log.Infof("list")

	kVStoreMutex.RLock()
	defer kVStoreMutex.RUnlock()
	for key, value := range keyValueStore {
		fmt.Fprintln(w, key, ":", value)
	}
}
