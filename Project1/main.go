package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
    var (
        serviceTime     int64
        totalWait       float64
        totalTurnaround float64
        lastCompletion  float64
        schedule        = make([][]string, len(processes))
        gantt           = make([]TimeSlice, 0)
    )
    // Create a priority queue for SJF Priority scheduling
    priorityQueue := make([]Process, 0)

    for serviceTime < lastCompletion {
        // Add processes that have arrived to the priority queue
        for _, p := range processes {
            if p.ArrivalTime <= serviceTime {
                priorityQueue = append(priorityQueue, p)
            }
        }

        // Sort the priority queue by priority (lower value indicates higher priority)
        sort.SliceStable(priorityQueue, func(i, j int) bool {
            return priorityQueue[i].Priority < priorityQueue[j].Priority
        })

        if len(priorityQueue) == 0 {
            // If no process is available, increment service time
            serviceTime++
            continue
        }

        // Get the next process with the highest priority
        nextProcess := priorityQueue[0]

        // Remove the process from the priority queue
        priorityQueue = priorityQueue[1:]

        // Calculate waiting time
        waitingTime := max(0, serviceTime-nextProcess.ArrivalTime)

        // Update metrics
        totalWait += float64(waitingTime)
        totalTurnaround += float64(waitingTime + nextProcess.BurstDuration)

        // Update the Gantt chart
        start := serviceTime
        completion := serviceTime + nextProcess.BurstDuration
        gantt = append(gantt, TimeSlice{
            PID:   nextProcess.ProcessID,
            Start: start,
            Stop:  completion,
        })

        // Update the schedule table
        schedule[nextProcess.ProcessID-1] = []string{
            fmt.Sprint(nextProcess.ProcessID),
            fmt.Sprint(nextProcess.Priority),
            fmt.Sprint(nextProcess.BurstDuration),
            fmt.Sprint(nextProcess.ArrivalTime),
            fmt.Sprint(waitingTime),
            fmt.Sprint(waitingTime + nextProcess.BurstDuration),
            fmt.Sprint(completion),
        }

        // Update the current time
        serviceTime = completion
    }

    // Calculate metrics
    count := float64(len(processes))
    aveWait := totalWait / count
    aveTurnaround := totalTurnaround / count
    aveThroughput := count / lastCompletion

    // Output the results
    outputTitle(w, title)
    outputGantt(w, gantt)
    outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}



func SJFSchedule(w io.Writer, title string, processes []Process) {
    var (
        serviceTime     int64
        totalWait       float64
        totalTurnaround float64
        lastCompletion  float64
        schedule        = make([][]string, len(processes))
        gantt           = make([]TimeSlice, 0)
    )
    // Create a priority queue for SJF scheduling
    priorityQueue := make([]Process, 0)

    for serviceTime < lastCompletion {
        // Add processes that have arrived to the priority queue
        for _, p := range processes {
            if p.ArrivalTime <= serviceTime {
                priorityQueue = append(priorityQueue, p)
            }
        }

        // Sort the priority queue by burst duration (SJF)
        sort.SliceStable(priorityQueue, func(i, j int) bool {
            return priorityQueue[i].BurstDuration < priorityQueue[j].BurstDuration
        })

        // Get the next process with the shortest burst duration
        nextProcess := priorityQueue[0]

        // Remove the process from the priority queue
        priorityQueue = priorityQueue[1:]

        // Calculate waiting time
        waitingTime := max(0, serviceTime-nextProcess.ArrivalTime)

        // Update metrics
        totalWait += float64(waitingTime)
        totalTurnaround += float64(waitingTime + nextProcess.BurstDuration)

        // Update the Gantt chart
        start := serviceTime
        completion := serviceTime + nextProcess.BurstDuration
        gantt = append(gantt, TimeSlice{
            PID:   nextProcess.ProcessID,
            Start: start,
            Stop:  completion,
        })

        // Update the schedule table
        schedule[nextProcess.ProcessID-1] = []string{
            fmt.Sprint(nextProcess.ProcessID),
            fmt.Sprint(nextProcess.Priority),
            fmt.Sprint(nextProcess.BurstDuration),
            fmt.Sprint(nextProcess.ArrivalTime),
            fmt.Sprint(waitingTime),
            fmt.Sprint(waitingTime + nextProcess.BurstDuration),
            fmt.Sprint(completion),
        }

        // Update the current time
        serviceTime = completion
    }

    // Calculate metrics
    count := float64(len(processes))
    aveWait := totalWait / count
    aveTurnaround := totalTurnaround / count
    aveThroughput := count / lastCompletion

    // Output the results
    outputTitle(w, title)
    outputGantt(w, gantt)
    outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
    var (
        quantum         int64 = 2 // Adjust the time quantum as needed
        totalWait       float64
        totalTurnaround float64
        schedule        = make([][]string, len(processes))
        gantt           = make([]TimeSlice, 0)
        currentTime     int64
        remainingBurst  = make(map[int64]int64)
    )

    // Initialize remaining burst times for each process
    for _, p := range processes {
        remainingBurst[p.ProcessID] = p.BurstDuration
    }

    for len(remainingBurst) > 0 {
        for pid, burst := range remainingBurst {
            if burst <= quantum {
                // Process completes within the time quantum
                start := currentTime
                currentTime += burst
                completion := currentTime

                // Update metrics
                totalWait += float64(currentTime - processes[pid-1].ArrivalTime - processes[pid-1].BurstDuration)
                totalTurnaround += float64(currentTime - processes[pid-1].ArrivalTime)

                // Add the process to the Gantt chart
                gantt = append(gantt, TimeSlice{
                    PID:   pid,
                    Start: start,
                    Stop:  completion,
                })

                // Add the process to the schedule table
                schedule[pid-1] = []string{
                    fmt.Sprint(pid),
                    fmt.Sprint(processes[pid-1].Priority),
                    fmt.Sprint(processes[pid-1].BurstDuration),
                    fmt.Sprint(processes[pid-1].ArrivalTime),
                    fmt.Sprint(currentTime - processes[pid-1].ArrivalTime - processes[pid-1].BurstDuration),
                    fmt.Sprint(currentTime - processes[pid-1].ArrivalTime),
                    fmt.Sprint(currentTime),
                }

                // Remove the completed process from the remaining burst map
                delete(remainingBurst, pid)
            } else {
                // Process continues execution, but quantum expires
                start := currentTime
                currentTime += quantum

                // Update remaining burst time for the process
                remainingBurst[pid] -= quantum

                // Add the process to the Gantt chart
                gantt = append(gantt, TimeSlice{
                    PID:   pid,
                    Start: start,
                    Stop:  currentTime,
                })
            }
        }
    }

    // Calculate metrics
    count := float64(len(processes))
    aveWait := totalWait / count
    aveTurnaround := totalTurnaround / count
    aveThroughput := count / float64(currentTime)

    // Output the results
    outputTitle(w, title)
    outputGantt(w, gantt)
    outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//func SJFPrioritySchedule(w io.Writer, title string, processes []Process) { }
//
//func SJFSchedule(w io.Writer, title string, processes []Process) { }
//
//func RRSchedule(w io.Writer, title string, processes []Process) { }

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
