package queue

import (
	"fmt"
	"reflect"
	"sync"

	custom_err "github.com/Viet-ph/redis-go/internal/error"
)

// import "github.com/Viet-ph/redis-go/internal/connection"

type Task struct {
	//conn     *connection.Conn
	callback any
	args     []any
}

func NewTask(cb any, args ...any) *Task {
	return &Task{
		callback: cb,
		args:     args,
	}
}
func (task *Task) Execute() error {
	fnValue := reflect.ValueOf(task.callback)
	inputs := make([]reflect.Value, len(task.args))
	for i, arg := range task.args {
		inputs[i] = reflect.ValueOf(arg)
	}

	results := fnValue.Call(inputs)
	// Convert results to []any
	outputs := make([]any, len(results))
	for i, result := range results {
		outputs[i] = result.Interface()
	}

	// TODO: Do smth with the return results other than error.
	// Check if the last return value is an error
	// Here only return the error if any
	if len(outputs) > 0 {
		if lastResult, ok := outputs[len(results)-1].(error); ok {
			// If the last result is of type error, return it
			return lastResult
		}
	}

	return nil
}

type TaskQueue struct {
	tasks []Task
	mut   sync.Mutex
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		tasks: make([]Task, 0),
	}
}

func (tq *TaskQueue) Add(task Task) {
	defer tq.mut.Unlock()

	tq.mut.Lock()
	tq.tasks = append(tq.tasks, task)
}

func (tq *TaskQueue) DrainQueue() {
	defer tq.mut.Unlock()

	tq.mut.Lock()
	if len(tq.tasks) == 0 {
		return
	}

	for len(tq.tasks) > 0 {
		fmt.Printf("Number of tasks: %d\n", len(tq.tasks))
		task := tq.tasks[0]
		err := task.Execute()
		if err != nil {
			fmt.Println("Error while executing task: " + err.Error())
			if err == custom_err.ErrorRequeueTask {
				tq.tasks = append(tq.tasks, task)
			} else {
				fmt.Println("Task executed with failure.")
				continue
			}
		}
		tq.tasks = tq.tasks[1:]
	}
}
