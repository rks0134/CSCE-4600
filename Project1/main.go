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

// func SJFPrioritySchedule(w io.Writer, title string, processes []Process) { }
//
// SJFPrioritySchedule implements Shortest Job First Priority (SJF Priority) scheduling algorithm.
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	remainingBurst := make([]int64, len(processes))
	copy(remainingBurst, getBurstDurations(processes))

	completion := int64(0)
	for completion < int64(len(processes)) {
		nextProcess := -1

		for i, p := range processes {
			if remainingBurst[i] > 0 && p.ArrivalTime <= completion {
				if nextProcess == -1 || remainingBurst[i] < remainingBurst[nextProcess] {
					nextProcess = i
				}
			}
		}

		if nextProcess == -1 {
			completion++
			continue
		}

		start := completion
		serviceTime++
		remainingBurst[nextProcess]--

		if remainingBurst[nextProcess] == 0 {
			turnaround := completion - processes[nextProcess].ArrivalTime + 1
			totalTurnaround += float64(turnaround)
			waitingTime = turnaround - processes[nextProcess].BurstDuration
			totalWait += float64(waitingTime)
			lastCompletion = float64(completion) + 1

			schedule[nextProcess] = []string{
				fmt.Sprint(processes[nextProcess].ProcessID),
				fmt.Sprint(processes[nextProcess].Priority),
				fmt.Sprint(processes[nextProcess].BurstDuration),
				fmt.Sprint(processes[nextProcess].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion + processes[nextProcess].BurstDuration),
			}
			completion += int64(processes[nextProcess].BurstDuration)
			gantt = append(gantt, TimeSlice{
				PID:   processes[nextProcess].ProcessID,
				Start: start,
				Stop:  completion,
			})
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// SJFSchedule implements Shortest Job First (SJF) scheduling algorithm.
// SJFSchedule implements Shortest Job First (SJF) scheduling algorithm.
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	remainingBurst := make([]int64, len(processes))
	copy(remainingBurst, getBurstDurations(processes))

	completion := int64(0)
	for completion < int64(len(processes)) {
		nextProcess := -1

		for i, p := range processes {
			if remainingBurst[i] > 0 && p.ArrivalTime <= completion {
				if nextProcess == -1 || remainingBurst[i] < remainingBurst[nextProcess] {
					nextProcess = i
				}
			}
		}

		if nextProcess == -1 {
			completion++
			continue
		}

		start := completion
		serviceTime++
		remainingBurst[nextProcess]--

		if remainingBurst[nextProcess] == 0 {
			turnaround := completion - processes[nextProcess].ArrivalTime + 1
			totalTurnaround += float64(turnaround)
			waitingTime = turnaround - processes[nextProcess].BurstDuration
			totalWait += float64(waitingTime)
			lastCompletion = float64(completion) + 1

			schedule[nextProcess] = []string{
				fmt.Sprint(processes[nextProcess].ProcessID),
				fmt.Sprint(processes[nextProcess].Priority),
				fmt.Sprint(processes[nextProcess].BurstDuration),
				fmt.Sprint(processes[nextProcess].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion + processes[nextProcess].BurstDuration),
			}
			completion += int64(processes[nextProcess].BurstDuration)
			gantt = append(gantt, TimeSlice{
				PID:   processes[nextProcess].ProcessID,
				Start: start,
				Stop:  completion,
			})
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// func SJFSchedule(w io.Writer, title string, processes []Process) { }
//
// func RRSchedule(w io.Writer, title string, processes []Process) { }
// RRSchedule implements Round-Robin (RR) scheduling algorithm.
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	remainingBurst := make([]int64, len(processes))
	copy(remainingBurst, getBurstDurations(processes))

	quantum := int64(2) // Adjust the quantum as needed

	completion := int64(0)                   // Change to int64
	for completion < int64(len(processes)) { // Change to int64
		for i := range processes {
			if remainingBurst[i] > 0 && processes[i].ArrivalTime <= completion { // Change to int64
				start := completion // Change to int64
				serviceTime++
				if remainingBurst[i] <= quantum {
					waitingTime = completion - processes[i].ArrivalTime
					turnaround := waitingTime + remainingBurst[i]
					totalWait += float64(waitingTime)
					totalTurnaround += float64(turnaround)
					lastCompletion = float64(completion) + float64(remainingBurst[i])

					schedule[i] = []string{
						fmt.Sprint(processes[i].ProcessID),
						fmt.Sprint(processes[i].Priority),
						fmt.Sprint(processes[i].BurstDuration),
						fmt.Sprint(processes[i].ArrivalTime),
						fmt.Sprint(waitingTime),
						fmt.Sprint(turnaround),
						fmt.Sprint(completion + remainingBurst[i]),
					}
					completion += int64(processes[i].BurstDuration)
					gantt = append(gantt, TimeSlice{
						PID:   processes[i].ProcessID,
						Start: start,
						Stop:  completion, // Change to int64
					})
					remainingBurst[i] = 0
				} else {
					waitingTime = completion - processes[i].ArrivalTime
					turnaround := waitingTime + quantum
					totalWait += float64(waitingTime)
					totalTurnaround += float64(turnaround)
					lastCompletion = float64(completion) + float64(quantum)

					schedule[i] = []string{
						fmt.Sprint(processes[i].ProcessID),
						fmt.Sprint(processes[i].Priority),
						fmt.Sprint(processes[i].BurstDuration),
						fmt.Sprint(processes[i].ArrivalTime),
						fmt.Sprint(waitingTime),
						fmt.Sprint(turnaround),
						fmt.Sprint(completion + quantum),
					}
					completion += quantum
					gantt = append(gantt, TimeSlice{
						PID:   processes[i].ProcessID,
						Start: start,
						Stop:  completion, // Change to int64
					})
					remainingBurst[i] -= quantum
				}
			}
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}
func getBurstDurations(processes []Process) []int64 {
	bursts := make([]int64, len(processes))
	for i, p := range processes {
		bursts[i] = p.BurstDuration
	}
	return bursts
}

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
