package notifier

var Todo chan int

type Notifier struct {
}

func init() {
	Todo = make(chan int)
}

func (t *Notifier) Notify(id int, reply *int) error {
	Todo <- id
	return nil
}
