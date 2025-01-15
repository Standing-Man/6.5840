package raft

type Entry struct {
	Term    int
	Command interface{}
}

type Logs struct {
	Entries    []Entry
	StartIndex int
}

func New(index, term int) Logs {
	return Logs{
		Entries: []Entry{
			{
				Term:    term,
				Command: nil,
			},
		},
		StartIndex: index,
	}
}

// Return the Length of Log
func (l *Logs) Len() int {
	return len(l.Entries) + l.StartIndex
}

// Return the Term of the log at index index
func (l *Logs) GetTermByIndex(index int) int {
	if index < l.StartIndex || index > l.Len() {
		panic("failed to get the term by index")
	}
	return l.Entries[index-l.StartIndex].Term
}

func (l *Logs) LastLogsIndex() int {
	return l.StartIndex + l.Len() - 1
}

// Return the Command of the log at index index
func (l *Logs) GetCommandByIndex(index int) interface{} {
	if index < l.StartIndex || index >= l.Len() {
		panic("failed to get the Command by index")
	}
	return l.Entries[index-l.StartIndex].Command
}

// Append new Entry into Logs
func (l *Logs) AppendEntry(entry Entry) {
	l.Entries = append(l.Entries, entry)
}

// Append new Entries into Logs
func (l *Logs) AppendEntries(entries []Entry) {
	l.Entries = append(l.Entries, entries...)
}

func (l *Logs) CutEnd(index int) {
	l.Entries = l.Entries[0 : index-l.StartIndex]
}

func (l *Logs) GetPrefix(index int) []Entry {
	return l.Entries[0 : index-l.StartIndex]
}

func (l *Logs) CutStart(index int) {
	l.Entries = l.Entries[index-l.StartIndex:]
	l.StartIndex = index
}

func (l *Logs) GetSuffix(index int) []Entry {
	return l.Entries[index-l.StartIndex:]
}
