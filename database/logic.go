package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/pmalek/image_service/task"
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) GetById(w http.ResponseWriter, r *http.Request) {
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

	id, err := strconv.Atoi(string(values.Get("id")))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err)
		return
	}

	datastoreMutex.RLock()
	bIsInError := err != nil || id >= len(datastore) // Reading the length of a slice must be done in a synchronized manner. That's why the mutex is used.
	datastoreMutex.RUnlock()

	if bIsInError {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Wrong input")
		return
	}

	datastoreMutex.RLock()
	value := datastore[id]
	datastoreMutex.RUnlock()

	response, err := json.Marshal(value)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err)
		return
	}

	fmt.Fprint(w, string(response))
}

func (h *Handler) NewTask(w http.ResponseWriter, r *http.Request) {
	datastoreMutex.Lock()
	taskToAdd := task.Task{
		Id:    len(datastore),
		State: 0,
	}
	datastore[taskToAdd.Id] = taskToAdd
	datastoreMutex.Unlock()

	fmt.Fprint(w, taskToAdd.Id)
}

func (h *Handler) GetNewTask(w http.ResponseWriter, r *http.Request) {
	noTasks := false

	datastoreMutex.RLock()
	if len(datastore) == 0 {
		noTasks = true
	}
	datastoreMutex.RUnlock()

	if noTasks {
		w.WriteHeader(http.StatusNoContent)
		fmt.Fprint(w, "Info: No non-started task.")
		log.Infof("No non-started task.")
		return
	}

	taskToSend := task.Task{Id: -1, State: 0}

	oNFTMutex.Lock()
	datastoreMutex.Lock()
	for i := oldestNotFinishedTask; i < len(datastore); i++ {
		if datastore[i].State == 2 && i == oldestNotFinishedTask {
			oldestNotFinishedTask++
			continue
		}
		if datastore[i].State == 0 {
			datastore[i] = task.Task{Id: i, State: 1}
			taskToSend = datastore[i]
			break
		}
	}
	datastoreMutex.Unlock()
	oNFTMutex.Unlock()

	if taskToSend.Id == -1 {
		w.WriteHeader(http.StatusNoContent)
		fmt.Fprint(w, "Info: No non-started task.")
		log.Infof("No non-started task.")
		return
	}

	myId := taskToSend.Id

	go func() {
		time.Sleep(time.Second * 120)
		datastoreMutex.Lock()
		if datastore[myId].State == 1 {
			datastore[myId] = task.Task{Id: myId, State: 0}
		}
		datastoreMutex.Unlock()
	}()

	response, err := json.Marshal(taskToSend)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err)
		return
	}

	fmt.Fprint(w, string(response))
}

func (h *Handler) FinishTask(w http.ResponseWriter, r *http.Request) {
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

	id, err := strconv.Atoi(string(values.Get("id")))

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err)
		return
	}

	updatedTask := task.Task{Id: id, State: 2}

	bErrored := false

	datastoreMutex.Lock()
	if datastore[id].State == 1 {
		datastore[id] = updatedTask
	} else {
		bErrored = true
	}
	datastoreMutex.Unlock()

	if bErrored {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Wrong input")
		return
	}

	fmt.Fprint(w, "success")
}

func (h *Handler) SetById(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err)
		return
	}

	taskToSet := task.Task{}
	err = json.Unmarshal([]byte(data), &taskToSet)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, err)
		return
	}

	bErrored := false
	datastoreMutex.Lock()
	if taskToSet.Id >= len(datastore) || taskToSet.State > 2 || taskToSet.State < 0 {
		bErrored = true
	} else {
		datastore[taskToSet.Id] = taskToSet
	}
	datastoreMutex.Unlock()

	if bErrored {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Wrong input")
		return
	}

	fmt.Fprint(w, "success")
}

func (h *Handler) List(w http.ResponseWriter, r *http.Request) {
	datastoreMutex.RLock()
	defer datastoreMutex.RUnlock()
	for key, value := range datastore {
		fmt.Fprintln(w, key, ": ", "id:", value.Id, " state:", value.State)
	}
}
